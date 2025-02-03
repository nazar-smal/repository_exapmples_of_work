WITH

ecomm_waterfall_by_member_cte AS (
    SELECT * FROM {{ref('ecomm_waterfall_by_member')}}
),

summary_cte AS (

    SELECT
        m.month,
        m.plan,
        m.billing_interval,
        m.is_active,
        m.after_2018_signup,
        m.is_activated,
        m.is_prime_customer,
        m.change,
        SUM(m.mrr) AS mrr,
        SUM(m.mrr_change) AS mrr_change,
        SUM(m.mrr) * 12 AS arr,
        SUM(m.mrr_change) * 12 AS arr_change,
        COUNT(DISTINCT m.member_id) AS count_customers
    FROM ecomm_waterfall_by_member_cte m
    {{ dbt_utils.group_by(n=8) }}
),

ending_cte AS (

    SELECT DISTINCT
        s.month,
        '8_Ending' AS metrics,
        s.plan,
        s.billing_interval,
        s.is_active,
        s.is_prime_customer,
        s.after_2018_signup,
        s.is_activated,
        SUM(s.arr) AS arr,
        SUM(s.arr) AS arr_change,
        SUM(s.count_customers) AS count_customers
    FROM summary_cte s
    WHERE s.change <> 'churn'
    {{ dbt_utils.group_by(n=8) }}
)

SELECT
    DATEADD('month', 1, e.month) AS month,
    '1_Starting' AS metrics,
    e.plan,
    e.billing_interval,
    e.is_active,
    e.is_prime_customer,
    e.after_2018_signup,
    e.is_activated,
    e.arr,
    e.arr_change,
    e.count_customers
FROM ending_cte e
WHERE DATEADD('month', 1, e.month) < DATE_TRUNC('month', '{{get_data_interval_end()}}'::DATE)

UNION ALL

SELECT *
FROM ending_cte e

UNION ALL

SELECT DISTINCT
    s.month,
    '2_Net_New' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'net_new'
{{ dbt_utils.group_by(n=8) }}

UNION ALL

SELECT DISTINCT
    s.month,
    '3_Expansion' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'expansion'
{{ dbt_utils.group_by(n=8) }}

UNION ALL

SELECT DISTINCT
    s.month,
    '4_Pricing' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'price_increase'
{{ dbt_utils.group_by(n=8) }}

UNION ALL

SELECT DISTINCT
    s.month,
    '5_Contraction' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'contraction'
{{ dbt_utils.group_by(n=8) }}

UNION ALL

SELECT DISTINCT
    s.month,
    '6_Churn' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'churn'
{{ dbt_utils.group_by(n=8) }}

UNION ALL

SELECT DISTINCT
    s.month,
    '7_Win_Back' AS metrics,
    s.plan,
    s.billing_interval,
    s.is_active,
    s.is_prime_customer,
    s.after_2018_signup,
    s.is_activated,
    SUM(s.arr) AS arr,
    SUM(s.arr_change) AS arr_change,
    SUM(s.count_customers) AS count_customers
FROM summary_cte s
WHERE s.change = 'win_back'
{{ dbt_utils.group_by(n=8) }}
