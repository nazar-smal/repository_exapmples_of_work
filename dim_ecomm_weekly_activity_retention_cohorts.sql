
WITH icp_by_member_cte AS (
    SELECT member_id,
    icp_tiers
    FROM {{ref('customers')}}
),

weekly_active_customers_cte AS (
    SELECT * FROM {{ref('weekly_active_customers')}}
),

monthly_active_customers_cte AS (
    SELECT * FROM {{ref('monthly_active_customers')}}
),

ecomm_customer_retention_by_member_cte AS (
    SELECT * FROM {{ref('ecomm_customer_retention_by_member')}}
),

ecomm_revenue_retention_by_member_cte AS (
    SELECT * FROM {{ref('ecomm_revenue_retention_by_member')}}
),

weekly_paying_members_cte AS (
    SELECT DISTINCT 
    DATE_TRUNC('week',s.date) AS date,
    DATE_TRUNC('month',DATE_TRUNC('week',s.date)) AS month,
    member_id,
    plan,
    billing_interval,
    market,
    country,
    trial_type,
    trial_length,
    acquisition_source,
    signup_source,
    is_tier_1_icp_member,
    icp_tiers
    FROM {{ref('ecomm_daily_paying_customers_by_member')}} s
    WHERE 1 + 1
    QUALIFY ROW_NUMBER() OVER(PARTITION BY member_id, DATE_TRUNC('week',s.date) ORDER BY s.date DESC) = 1
),

monthly_paying_members_cte AS (
    SELECT DISTINCT 
    month,
    member_id
    FROM weekly_paying_members_cte
),

monthly_retention_cte AS (
    SELECT p.member_id,
    p.month,
    CASE WHEN m.is_active IS NOT NULL THEN 1 ELSE 0 END AS is_active_ind_m,
    SUM(is_active_ind_m) OVER(PARTITION BY p.member_id ORDER BY p.month ASC ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS twelve_month_rolling_active_count,
    SUM(is_active_ind_m) OVER(PARTITION BY p.member_id ORDER BY p.month ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS two_month_rolling_active_count
    FROM monthly_paying_members_cte p
    LEFT JOIN monthly_active_customers_cte m ON m.member_id = p.member_id AND m.month_of_activity = p.month
),

weekly_retention_cte AS (
    SELECT p.member_id,
    p.date,
    icp.icp_tiers,
    p.plan,
    p.billing_interval,
    p.market,
    p.country,
    p.trial_type,
    p.acquisition_source,
    p.signup_source,
    c.is_customer_retained,
    c.is_customer_retained_and_active,
    r.net_arr_retained,
    r.gross_arr_retained,
    CASE WHEN w.is_active IS NOT NULL THEN 1 ELSE 0 END AS is_active_ind,
    SUM(is_active_ind) OVER(PARTITION BY p.member_id ORDER BY p.date ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS three_week_rolling_active_count,
    LAG(is_active_ind,1) OVER(PARTITION BY p.member_id ORDER BY p.date ASC) AS previous_week_is_active_ind,
    LAG(is_active_ind,2) OVER(PARTITION BY p.member_id ORDER BY p.date ASC) AS previous_two_weeks_is_active_ind,

    CASE WHEN three_week_rolling_active_count = 3 THEN '1_super_healthy'
         WHEN is_active_ind = 1 AND previous_week_is_active_ind IS NULL THEN '3_low_risk'
         WHEN is_active_ind = 1 AND three_week_rolling_active_count >= 2 THEN '2_healthy'
         WHEN is_active_ind = 1 AND three_week_rolling_active_count <= 1 THEN '4_medium_risk'
         WHEN is_active_ind = 0 AND previous_week_is_active_ind IS NULL THEN '4_medium_risk'
         WHEN is_active_ind = 0 AND three_week_rolling_active_count = 0 AND twelve_month_rolling_active_count = 0 THEN '7_dormant_12m'
         WHEN is_active_ind = 0 AND three_week_rolling_active_count = 0 AND two_month_rolling_active_count = 0 THEN '6_dormant_2m'
         WHEN is_active_ind = 0 AND three_week_rolling_active_count = 0 THEN '5_high_risk'
         WHEN is_active_ind = 0 AND three_week_rolling_active_count >= 2 THEN '3_low_risk'
         WHEN is_active_ind = 0 AND three_week_rolling_active_count = 1 THEN '4_medium_risk'
         END AS activity_retention_cohort
    FROM weekly_paying_members_cte p
    LEFT JOIN weekly_active_customers_cte w ON w.member_id = p.member_id AND w.week_of_activity = p.date
    LEFT JOIN monthly_retention_cte m ON m.member_id = p.member_id AND m.month = p.month
    LEFT JOIN icp_by_member_cte icp ON icp.member_id = p.member_id
    LEFT JOIN ecomm_customer_retention_by_member_cte c ON c.member_id = p.member_id AND c.date = p.month
    LEFT JOIN ecomm_revenue_retention_by_member_cte r ON r.member_id = p.member_id AND r.date = p.month
    WHERE p.date <= DATEADD('week',-1,DATE_TRUNC('week','{{get_data_interval_end()}}'::date))
)

SELECT member_id,
date,
icp_tiers,
plan,
billing_interval,
market,
country,
trial_type,
acquisition_source,
signup_source,
is_customer_retained,
is_customer_retained_and_active,
net_arr_retained,
gross_arr_retained,
is_active_ind AS is_active,
activity_retention_cohort
FROM weekly_retention_cte