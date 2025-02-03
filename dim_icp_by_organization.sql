WITH salesforce_account_cte AS (
	SELECT * FROM {{ source('salesforce' , 'account') }}
),

salesforce_organization_cte AS (
	SELECT * FROM {{ref('salesforce__organization')}}
),

sfdc_industry_mapping_cte AS (
	SELECT * FROM {{ ref('sfdc_industry_mapping') }}
),

sfdc_employee_range_mapping_cte AS (
	SELECT * FROM {{ ref('sfdc_employee_range_mapping') }}
),
-- accounts with 1 primary org
dedupe_accounts_by_primary_org_cte AS (
	SELECT id AS account_id
	FROM salesforce_account_cte
	WHERE organisation_id__c IS NOT NULL
	GROUP BY 1
	HAVING COUNT(DISTINCT organisation_id__c) = 1
),
-- primary orgs with 1 account
dedupe_primary_orgs_by_account_cte AS (
	SELECT organisation_id__c AS organization_id,
	MAX(id) AS account_id
    FROM salesforce_account_cte
	WHERE organisation_id__c IS NOT NULL
	GROUP BY 1
	HAVING COUNT(DISTINCT id) = 1
),
-- primary orgs with 1 account where the account has 1 primary org
dedupe_primary_orgs_cte AS (
	SELECT t.organization_id,
	t.account_id
	FROM dedupe_primary_orgs_by_account_cte t
	INNER JOIN dedupe_accounts_by_primary_org_cte u ON u.account_id = t.account_id
),
-- accounts with no priamry org
accounts_with_no_primary_org_cte AS (
	SELECT id AS account_id
	FROM salesforce_account_cte
	WHERE organisation_id__c IS NULL
),
-- orgs with 1 account
dedupe_orgs_cte AS (
	SELECT organization_id__c AS organization_id,
	MAX(account__c) AS account_id
	FROM salesforce_organization_cte
	GROUP BY 1
	HAVING COUNT(DISTINCT account__c) = 1
),
-- orgs with 1 primary org and 1 account 
dedupe_orgs_with_primary_org_cte AS (
	SELECT o.organization_id,
	o.account_id
	FROM dedupe_orgs_cte o
	INNER JOIN dedupe_primary_orgs_cte a ON a.account_id = o.account_id
),
-- orgs with 1 account and no primary org
dedupe_orgs_with_no_primary_org_cte AS (
	SELECT o.organization_id,
	o.account_id
	FROM dedupe_orgs_cte o
	INNER JOIN accounts_with_no_primary_org_cte a ON a.account_id = o.account_id
),

orgs_combined_cte AS (
	SELECT organization_id::VARCHAR AS organization_id,
	account_id
	FROM dedupe_primary_orgs_cte

	UNION

	SELECT organization_id::VARCHAR AS organization_id,
	account_id
	FROM dedupe_orgs_with_no_primary_org_cte

	UNION

	SELECT organization_id::VARCHAR AS organization_id,
	account_id
	FROM dedupe_orgs_with_primary_org_cte
),

-- orgs with 1 account id across all sources
unique_orgs_combined_cte AS (
	SELECT organization_id,
	MAX(account_id) AS account_id
	FROM orgs_combined_cte
	GROUP BY 1
	HAVING COUNT(DISTINCT account_id) = 1
),

full_org_list_combined AS (
	SELECT organisation_id__c::VARCHAR AS organization_id
	FROM salesforce_account_cte
	WHERE organisation_id__c IS NOT NULL
	
	UNION

	SELECT organization_id__c::VARCHAR AS organization_id 
	FROM salesforce_organization_cte
),

dedupe_org_base_properties_cte AS (
	SELECT m.organization_id,
	m.account_id,
	LAST_VALUE(l.icp_tier__c) IGNORE NULLS OVER(PARTITION BY m.account_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS icp_tier__c,
	LAST_VALUE(l.industry) IGNORE NULLS OVER(PARTITION BY m.account_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS industry__c,
	LAST_VALUE(l.no_of_employees__c) IGNORE NULLS OVER(PARTITION BY m.account_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS no_of_employees__c,
	LAST_VALUE(l.billingcountry) IGNORE NULLS OVER(PARTITION BY m.account_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS country__c
	FROM unique_orgs_combined_cte m
	LEFT JOIN salesforce_account_cte l ON  l.id = m.account_id
),

org_base_cte AS (
	SELECT p.organization_id,
	p.account_id,
	p.icp_tier__c AS icp_tiers,
	t.current_sf_industry AS industry,
	e.current_sf_emps AS no_of_employees,
	p.country__c AS country
	FROM dedupe_org_base_properties_cte p
	LEFT JOIN sfdc_employee_range_mapping_cte e ON e.legacy_sf_emps = p.no_of_employees__c
	LEFT JOIN sfdc_industry_mapping_cte t ON t.legacy_sf_industry = p.industry__c
),

final_cte AS (
	SELECT f.organization_id,
	b.account_id,
	b.country,
	b.industry,
	b.no_of_employees,
	CASE WHEN b.icp_tiers LIKE '%1%' THEN 'tier_1'
         WHEN b.icp_tiers LIKE '%2%' THEN 'tier_2'
	     WHEN b.icp_tiers LIKE '%3%' THEN 'tier_3'
	     WHEN b.icp_tiers LIKE '%4%' THEN 'tier_4'
	ELSE 'unknown'
	END AS icp_tiers
	FROM full_org_list_combined f
	LEFT JOIN org_base_cte b ON b.organization_id = f.organization_id
)

SELECT organization_id,
account_id,
country,
industry,
no_of_employees,
icp_tiers
FROM final_cte
