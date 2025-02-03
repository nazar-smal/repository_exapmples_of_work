WITH

iso_dates_cte AS (
    SELECT * FROM {{ref('standards__iso_dates')}}
),

paid_base_by_member_cte AS (
    SELECT * FROM {{ref('paid_base_by_member')}}
),

mrr_by_member_cte AS (
    SELECT * FROM {{ref('mrr_by_member')}}
),

monthly_active_customers_cte AS (
    SELECT * FROM {{ref('monthly_active_customers')}}
),

icp_by_member_cte AS (
    SELECT * FROM {{ref('icp_by_member')}}
),

ecomm_activated_by_member_cte AS (
    SELECT * FROM {{ref('ecomm_activated_by_member')}}
),

email_domain_cte AS (
    SELECT * FROM {{ref('growth__email_domain_by_member_id')}}
),

member_enhanced_cte AS (
    SELECT * FROM {{ref('hootsuite_enhanced__member_enhanced') }}
),

month_shell_cte AS (

    SELECT DATE_TRUNC('month', i.date) AS mrr_month
    FROM iso_dates_cte i
    {{ dbt_utils.group_by(n=1) }}
    HAVING mrr_month >= '2010-08-01' AND mrr_month < DATE_TRUNC('month', '{{get_data_interval_end()}}'::DATE)
),

member_mrr_cte AS (

    SELECT DISTINCT
        p.member_id,
        p.date AS payment_month,
        p.plan,
        p.billing_interval,
        COALESCE(a.is_active, FALSE) AS is_active,
        i.is_tier_1_icp_member,
        CASE WHEN ac.activated = 1 THEN TRUE ELSE FALSE END AS is_activated,
        SUM(m.mrr) AS mrr
    FROM paid_base_by_member_cte p
    LEFT JOIN mrr_by_member_cte m ON p.member_id = m.member_id AND p.date = m.date
    LEFT JOIN monthly_active_customers_cte a ON p.member_id = a.member_id AND p.date = a.month_of_activity
    LEFT JOIN icp_by_member_cte i ON p.member_id = i.member_id
    LEFT JOIN ecomm_activated_by_member_cte ac ON p.member_id = ac.member_id
    {{ dbt_utils.group_by(n=7) }}
),

member_first_payment_cte AS (

    SELECT DISTINCT
        m.member_id,
        MIN(payment_month) AS first_payment_month
    FROM member_mrr_cte m
    {{ dbt_utils.group_by(n=1) }}
),

mrr_date_shell_cte AS (

    SELECT DISTINCT
        m.member_id,
        s.mrr_month
    FROM month_shell_cte s
    INNER JOIN member_first_payment_cte m ON s.mrr_month >= m.first_payment_month
),

mrr_bins_cte AS (
    SELECT DISTINCT
        ds.member_id,
        ds.mrr_month,
        mm.plan,
        mm.billing_interval,
        mm.is_active,
        mm.is_tier_1_icp_member,
        mm.mrr AS this_month_mrr,
        LAG(this_month_mrr, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_month_mrr,
        LAG(mm.plan, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_month_plan,
        LAG(mm.billing_interval, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_month_billing_interval,
        LAG(mm.is_tier_1_icp_member, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_is_tier_1_icp_member,
        LAG(mm.is_active, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_month_is_active,
        CASE
            WHEN g.member_id THEN 'net_new'
            WHEN last_month_mrr > 0 AND this_month_mrr > last_month_mrr AND mm.plan = last_month_plan AND mm.billing_interval = last_month_billing_interval THEN 'price_increase'
            WHEN last_month_mrr > 0 AND this_month_mrr > last_month_mrr THEN 'expansion'
            WHEN last_month_mrr IS NULL AND this_month_mrr > 0 THEN 'win_back'
            WHEN this_month_mrr <> 0 AND last_month_mrr <> 0 AND last_month_mrr > this_month_mrr THEN 'contraction'
            WHEN mm.member_id IS NULL AND last_month_mrr > 0 THEN 'churn'
            WHEN this_month_mrr < 0 THEN 'contraction'
            WHEN this_month_mrr <> 0 AND this_month_mrr = last_month_mrr THEN 'previous_month'
            WHEN this_month_mrr <> 0 THEN 'expansion'
        END AS change,
        CASE
            WHEN change IN ('net_new', 'price_increase', 'expansion', 'win_back', 'contraction') THEN this_month_mrr
            WHEN change = 'previous_month' THEN last_month_mrr
            WHEN change = 'churn' THEN -1 * last_month_mrr
        END AS mrr1,
        CASE
            WHEN change IN ('net_new', 'win_back') THEN this_month_mrr
            WHEN change IN ('expansion', 'contraction', 'price_increase', 'previous_month') THEN this_month_mrr - last_month_mrr
            WHEN change = 'churn' THEN -1 * last_month_mrr
        END AS mrr_change1,
        COALESCE(mm.plan, last_month_plan) AS plan1,
        COALESCE(mm.billing_interval, last_month_billing_interval) AS billing_interval1,
        COALESCE(mm.is_tier_1_icp_member, last_is_tier_1_icp_member) AS is_tier_1_icp_member1,
        COALESCE(CASE WHEN change = 'churn' THEN last_month_is_active ELSE mm.is_active END, FALSE) AS is_active1,
        CASE WHEN g.first_payment_month >= '2018-01-01' THEN TRUE ELSE FALSE END AS after_2018_signup,
        mm.is_activated,
        LAG(mm.is_activated, 1) OVER (PARTITION BY ds.member_id ORDER BY ds.mrr_month ASC) AS last_month_is_activated,
        COALESCE(CASE WHEN change = 'churn' THEN last_month_is_activated ELSE mm.is_activated END, FALSE) AS is_activated1
    FROM mrr_date_shell_cte ds
    LEFT JOIN member_mrr_cte mm ON ds.member_id = mm.member_id AND ds.mrr_month = mm.payment_month
    LEFT JOIN member_first_payment_cte g ON mm.member_id = g.member_id AND mm.payment_month = first_payment_month
)

SELECT DISTINCT
    m.member_id,
    m.mrr_month AS month,
    f.first_payment_month,
    m.plan1 AS plan,
    m.last_month_plan,
    m.billing_interval1 AS billing_interval,
    m.last_month_billing_interval,
    COALESCE(m.is_active1, FALSE) AS is_active,
    m.last_month_is_active,
    m.after_2018_signup,
    COALESCE(m.is_activated1, FALSE) AS is_activated,
    COALESCE(m.is_tier_1_icp_member1, FALSE) AS is_prime_customer,
    CASE
        WHEN LOWER(SPLIT_PART(e.email, '@', 2)) = 'hootsuite.com' THEN NULL
        ELSE LOWER(SPLIT_PART(e.email, '@', 2))
    END AS email_domain,
    e.email_type,
    i.country,
    i.industry,
    i.no_of_employees,
    i.company_revenue,
    me.market,
    m.change,
    SUM(m.mrr1) AS mrr,
    SUM(m.mrr_change1) AS mrr_change,
    mrr * 12 AS arr,
    mrr_change * 12 AS arr_change
FROM mrr_bins_cte m
LEFT JOIN member_first_payment_cte f ON m.member_id = f.member_id
LEFT JOIN icp_by_member_cte i ON m.member_id = i.member_id
LEFT JOIN email_domain_cte e ON m.member_id = e.member_id
LEFT JOIN member_enhanced_cte me ON m.member_id = me.member_id
WHERE m.change IS NOT NULL
{{ dbt_utils.group_by(n=20) }}
