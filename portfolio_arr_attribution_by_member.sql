/*****************************************************************************************
Portfolio _arr Attribution by Member
Owner: Nazar Smal
Created Date: Sep 2021
Purpose: Create table containing info on ARR attribution by product portfolios on the member_id or org_id levels based on frequencies (number of days) of usage of these portfolios within the calendar month.
Only active members are in the table, so if the org member was inactive they are not inculuded here, org ARR is distributed only between active members.
Organization ARR is attributed to member/portfolio level as a proportion of member/portfolio frequency out of sum of all member/portfolio frequencies within org.
Example: org ARR is $100 and there are 2 members in org. the first member just composed for 2 days. Second member used planner for 2 days and inbox for 6 days. Total member/portfolio days = 2+4+4=10 days. ARR per member/portfolio day is $100/10 days=$10. So the first member is attributed with $100*2 days=$20 ARR that all goes to composer.
Second member is attributed with $80 ARR which is split to $10*2=$20 to planner and $10*6=$60 to inbox.
ARR is rounded to 2 decimal points.

 	• date_moth - month
 	• member_id
 	• organization_id
 	• arr_attribution - ARR attributed to member based frequencies (number of days) of usage of different portfolios (see example above)
 	• amplify_arr_attribution - ARR attributed to amplify based on frequency of amplify usage comparing to freqeuencies of usage of other portfolios
	.....
	• streams_arr_attribution - ARR attributed to streams based on frequency of streams usage comparing to freqeuencies of usage of other portfolios

NOTE: Data is from 2018 and later.
*****************************************************************************************/


BEGIN;

SET SEARCH_PATH TO 'product';

TRUNCATE portfolio_arr_attribution_by_member;

INSERT INTO portfolio_arr_attribution_by_member

WITH org_frequency AS (

SELECT DISTINCT m.organization_id,
       m.date_month,
       SUM(m.amplify_days+m.analytics_days+m.apps_days+m.assignments_days+m.authentication_days+m.composer_days+m.inbox_days+m.planner_days+m.streams_days+m.social_ads_days+m.content_days+m.hootdesk_days) AS total_org_frequency

FROM product.monthly_active_users_by_member m

GROUP BY 1,2
),

---there are duplicates of orgs in the enterprise.account_arr_monthly with different arr for different org name with the same org_id
enterprise_revenue AS (

SELECT DISTINCT e.organization_id,
       DATE_TRUNC ('month',e.date) AS date_month,
       SUM (e.arr) AS arr

FROM enterprise.account_arr_monthly e

GROUP BY 1,2
),

--there are duplicates of the same member_ids on the different plans and different payments
self_serve_revenue AS (

SELECT DISTINCT r.member_id,
       DATE_TRUNC ('month',r.payment_month) AS date_month,
       SUM (r.mrr) AS mrr

FROM self_serve.monthly_revenue_by_member r

WHERE r.refunded_for_first_payment IS FALSE

GROUP BY 1,2
),

mau_arr AS (

SELECT DISTINCT m.date_month,
       m.member_id,
       m.organization_id,
       m.amplify_days+m.analytics_days+m.apps_days+m.assignments_days+m.authentication_days+m.composer_days+m.content_days+m.inbox_days+m.planner_days+m.social_ads_days+m.streams_days AS total_portfolio_frequency,
       m.segments,
       CASE WHEN m.segments<>'enterprise' THEN ROUND(r.mrr*12,2)
            WHEN o.total_org_frequency>0 THEN  ROUND(a.arr*total_portfolio_frequency*1.00/o.total_org_frequency,2)
            ELSE 0.00 END AS arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(amplify_days*1.00/total_portfolio_frequency*arr_attribution,2)---need case statements to avoid division by 0 when total frequency is 0, there is activity on other categoris not taken into account here for arr attribution
            ELSE 0.00 END AS amplify_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(analytics_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS analytics_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(apps_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS apps_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(assignments_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS assignments_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(authentication_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS authentication_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(composer_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS composer_arr_attribution,
      CASE WHEN total_portfolio_frequency>0 THEN ROUND(content_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS content_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(inbox_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS inbox_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(planner_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS planner_arr_attribution,
      CASE WHEN total_portfolio_frequency>0 THEN ROUND(social_ads_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS social_ads_arr_attribution,
       CASE WHEN total_portfolio_frequency>0 THEN ROUND(streams_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS streams_arr_attribution,
      CASE WHEN total_portfolio_frequency>0 THEN ROUND(hootdesk_days*1.00/total_portfolio_frequency*arr_attribution,2)
            ELSE 0.00 END AS hootdesk_arr_attribution


FROM product.monthly_active_users_by_member m
LEFT JOIN enterprise_revenue a ON a.organization_id=m.organization_id AND m.date_month=a.date_month
LEFT JOIN self_serve_revenue r ON m.member_id=r.member_id AND m.date_month=r.date_month
LEFT JOIN org_frequency o ON m.organization_id=o.organization_id AND m.date_month=o.date_month
)

SELECT DISTINCT m.date_month,
       m.member_id,
       m.organization_id,
       m.arr_attribution,
       m.amplify_arr_attribution,
       m.analytics_arr_attribution,
       m.apps_arr_attribution,
       m.assignments_arr_attribution,
       m.authentication_arr_attribution,
       m.composer_arr_attribution,
       m.inbox_arr_attribution,
       m.planner_arr_attribution,
       m.streams_arr_attribution,
       m.content_arr_attribution,
       m.social_ads_arr_attribution,
       m.hootdesk_arr_attribution



FROM mau_arr m
;
