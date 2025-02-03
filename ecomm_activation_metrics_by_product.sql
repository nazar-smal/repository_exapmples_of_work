WITH events_all_data_cte AS (
    SELECT *
    FROM {{ ref('events__event')}} e
    WHERE e.timestamp >= '2018-01-01'
),

buy_now_events_all_data_cte AS (
    SELECT *
    FROM {{ref('billing__buy_now_events')}}
),

customers_cte AS (
    SELECT {{dbt_utils.star(from=ref('ecomm_customers'),except=['first_payment_date',
    'trial_start_date',
    'trial_end_date',
    'last_touch_signup_date',
    'day_diff_first_payment_exp_conversion_date',
    'non_profit_effective_date'
    ])}}
    FROM {{ ref('ecomm_customers') }}
    WHERE
        DATE_PART('year', signup_date) >= 2018
),

discovery_event_all_data_cte AS (
    SELECT *
    FROM {{ ref('discovery_events')}}
),

ecomm_trial_to_paying_by_member_cte AS (
    SELECT * FROM {{ ref('ecomm_trial_to_paying_by_member')}}
),

meaningful_activity_all_data_cte AS (
    SELECT * FROM {{ref('meaningful_activity')}}

),

authentication_events_all_data_cte AS (
    SELECT * FROM {{ ref('authentication_events')}}

),

--Adding this CTE for "buynow" customers. In the ecomm_customers table, the signup_date for buynow customers
--is recorded as their first_payment_date. To track their activity accurately, use the 
--actual signup_date (when they first signed up) rather than the date of their first payment

events_buynow_data_cte AS (
    SELECT
        bu.member_id,
        MIN(bu.timestamp) AS buynow_signup_date
    FROM buy_now_events_all_data_cte bu
    WHERE bu.is_buy_now = TRUE
    GROUP BY 1
),

customers_all_data_cte AS (
    SELECT
        ecomm.*,
        COALESCE(ev.buynow_signup_date, ecomm.signup_date) AS adjusted_signup_date
    FROM customers_cte ecomm
    LEFT JOIN events_buynow_data_cte ev
        ON
            ecomm.member_id = ev.member_id AND
            ecomm.trial_or_buynow_signup = 'buynow'   
),

discovery_event_cte AS (
    SELECT
        e.*,
        disc.activity_category
    FROM events_all_data_cte e
    INNER JOIN discovery_event_all_data_cte disc ON e.event = disc.event
),

meaningful_activity_cte AS (
    SELECT DISTINCT
        CASE
            WHEN activity_category IN ('inbox_1', 'inbox_2') THEN 'inbox'
            ELSE activity_category
        END AS activity_category
    FROM meaningful_activity_all_data_cte

    UNION ALL

    SELECT 'any_product' AS activity_category

),

event_meaningful_cte AS (
    SELECT DISTINCT
        CASE WHEN a.activity_category ILIKE '%inbox%' THEN 'inbox' ELSE activity_category END AS activity_category,
        e.actor_id AS member_id,
        e.timestamp,
        e.event
    FROM events_all_data_cte e
    INNER JOIN meaningful_activity_all_data_cte a ON e.event = a.event
),

customer_base_cte AS (
    SELECT
        e.activity_category,
        a.*
    FROM customers_all_data_cte a
    CROSS JOIN meaningful_activity_cte e

),

authentication_events_cte AS (
    SELECT
        *,
        CASE
            WHEN LOWER(social_network_type) ILIKE '%facebook%' THEN 'facebook'
            WHEN LOWER(social_network_type) ILIKE '%instagram%' THEN 'instagram'
            WHEN LOWER(social_network_type) ILIKE '%linkedin%' THEN 'linkedin'
            WHEN LOWER(social_network_type) ILIKE '%hsapp%' THEN NULL
            ELSE LOWER(social_network_type)
        END AS social_network_type_group
    FROM authentication_events_all_data_cte
    WHERE
        authentication_result = 'success'
        AND authentication_type = 'authentication'
),
-----Set up CTE

final_setup_cte AS (
    SELECT
        c.member_id,
        CASE WHEN COUNT(DISTINCT social_network_type_group) >= 3 THEN 1 ELSE 0 END AS is_setup
    FROM customers_all_data_cte c
    LEFT JOIN authentication_events_cte e
        ON c.member_id = e.member_id
    WHERE e.timestamp BETWEEN c.adjusted_signup_date AND DATEADD('hour', 1, c.adjusted_signup_date)
    GROUP BY 1
),

-----Discovery CTE
discovery_product_cte AS (
    SELECT
        c.member_id,
        c.activity_category,
        COUNT(e.timestamp) AS count_timestamp
    FROM customer_base_cte c
    LEFT JOIN discovery_event_cte e
        ON c.member_id = e.actor_id
    WHERE
        e.timestamp BETWEEN c.adjusted_signup_date AND DATEADD('day', 3, c.adjusted_signup_date)
        AND c.activity_category = e.activity_category
    {{ dbt_utils.group_by(n=2) }}

),

event_product_discovery_cte AS (
    SELECT
        c.member_id,
        c.activity_category,
        CASE WHEN count_timestamp >= 1 THEN 1 ELSE 0 END AS is_discovery
    FROM discovery_product_cte c
    WHERE c.activity_category NOT IN ('any_product')
),

event_overall_discovery_cte AS (
    SELECT
        c.member_id,
        'any_product' AS activity_category,
        CASE WHEN SUM(is_discovery) >= 2 THEN 1 ELSE 0 END AS is_discovery
    FROM event_product_discovery_cte c
    {{ dbt_utils.group_by(n=2) }}
),


final_discovery_cte AS (

    SELECT *
    FROM event_product_discovery_cte

    UNION ALL

    SELECT *
    FROM event_overall_discovery_cte
),

-----Activated CTE
activated_product_cte AS (
    SELECT
        c.member_id,
        c.activity_category,
        COUNT(d.timestamp) AS count_timestamp
    FROM customer_base_cte c
    LEFT JOIN event_meaningful_cte d ON c.member_id = d.member_id
    WHERE
        d.timestamp BETWEEN c.adjusted_signup_date AND DATEADD('day', 7, c.adjusted_signup_date)
        AND c.activity_category = d.activity_category
    GROUP BY c.member_id, c.activity_category

),

event_product_activated_cte AS (
    SELECT
        c.member_id,
        c.activity_category,
        CASE WHEN count_timestamp >= 1 THEN 1 ELSE 0 END AS is_activated
    FROM activated_product_cte c
    WHERE c.activity_category NOT IN ('any_product')
),

event_overall_activated_cte AS (
    SELECT
        c.member_id,
        'any_product' AS activity_category,
        CASE WHEN SUM(is_activated) >= 2 THEN 1 ELSE 0 END AS is_activated
    FROM event_product_activated_cte c
    {{ dbt_utils.group_by(n=2) }}
),

final_activated_cte AS (
    SELECT *
    FROM event_product_activated_cte

    UNION ALL

    SELECT *
    FROM event_overall_activated_cte
),

----Final CTE
final_cte AS (
    SELECT
        c.*,
        CASE WHEN s.is_setup = 1 THEN 1 ELSE 0 END AS is_setup,
        CASE WHEN d.is_discovery = 1 THEN 1 ELSE 0 END AS is_discovery,
        CASE WHEN a.is_activated = 1 THEN 1 ELSE 0 END AS is_activated,
        CASE WHEN ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_converted,
        CASE WHEN s.is_setup = 1 AND ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_setup_and_converted,
        CASE WHEN d.is_discovery = 1 AND ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_discovery_and_converted,
        CASE WHEN a.is_activated = 1 AND ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_activated_and_converted,
        CASE WHEN s.is_setup = 1 AND d.is_discovery = 1 THEN 1 ELSE 0 END AS is_discovery_funnel,
        CASE WHEN s.is_setup = 1 AND d.is_discovery = 1 AND a.is_activated = 1 THEN 1 ELSE 0 END AS is_activated_funnel,
        CASE WHEN s.is_setup = 1 AND d.is_discovery = 1 AND ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_discovery_funnel_and_converted,
        CASE WHEN s.is_setup = 1 AND d.is_discovery = 1 AND a.is_activated = 1 AND ecomm.is_converted = TRUE THEN 1 ELSE 0 END AS is_activated_funnel_and_converted

    FROM customer_base_cte c
    LEFT JOIN final_setup_cte s ON s.member_id = c.member_id
    LEFT JOIN final_discovery_cte d ON d.member_id = c.member_id AND d.activity_category = c.activity_category
    LEFT JOIN final_activated_cte a ON a.member_id = c.member_id AND a.activity_category = c.activity_category
    LEFT JOIN ecomm_trial_to_paying_by_member_cte ecomm ON ecomm.member_id = c.member_id
)

SELECT * FROM final_cte
