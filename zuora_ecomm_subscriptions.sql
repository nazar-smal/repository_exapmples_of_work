WITH zuora_invoice_item_enhanced_cte AS (
    SELECT * FROM {{ ref('zuora_invoice_item_enhanced') }}
),

ecomm_customers_cte AS (
    SELECT * FROM {{ ref('ecomm_customers') }}
),

final_cte AS (
    SELECT
        {{dbt_utils.star(from=ref('zuora_invoice_item_enhanced'),relation_alias='i',except=['billing_system'])}},
        DATEADD(DAY, 1, LEAST(i.service_end_date, i.subscription_cancel_date))::DATE AS next_renewal_date,
        CASE WHEN i.service_end_date >= c.non_profit_effective_date::DATE THEN TRUE ELSE FALSE END AS is_non_profit,
        c.icp_tiers,
        c.market,
        c.country,
        c.language,
        c.signup_source,
        c.acquisition_source,
        c.trial_or_buynow_signup,
        c.trial_plan,
        c.trial_billing_interval,
        c.trial_length,
        c.trial_type,
        c.email_type,
        i.billing_system
    FROM zuora_invoice_item_enhanced_cte AS i
    LEFT JOIN ecomm_customers_cte AS c ON i.member_id = c.member_id
    WHERE 1 = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.original_rate_plan_charge_id ORDER BY i.version DESC) = 1
)

SELECT * FROM final_cte
