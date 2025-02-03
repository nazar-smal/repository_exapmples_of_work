/*
Experiments Monitoring
Created by: Nazar Smal
Edited by: Nika Charlton

Purpose: Creates an aggregated table that pipes the Experimentation Dashboard - a place for live tracking all acquisition and retention experiments
*/



TRUNCATE product.experiments_monitoring;

INSERT INTO product.experiments_monitoring AS (

WITH experiment_events AS (

SELECT g.member_id,
        g.experiment,
        g.variant,
        g.date,
        CASE WHEN g.experiment = 'GRW_SS_ACT_1' THEN 'free'
		WHEN (LEFT(UPPER(g.experiment),3)='EX_' 
		OR LEFT(UPPER(g.experiment),6)='GRW_SS' 
		OR LEFT(UPPER(g.experiment),3)='CA_'
		OR LEFT(UPPER(g.experiment),3)='HP_' 
		OR LEFT(UPPER(g.experiment),3)='PL_' 
		OR LEFT(LOWER(g.experiment),9)='sitewide_' 
		OR LEFT(UPPER(g.experiment),3)='DE_' 
		OR LEFT(UPPER(g.experiment),3)='PM_' 
		OR LEFT(UPPER(g.experiment),4)= 'PNC_')
			THEN 'paid'
		WHEN LEFT(UPPER(g.experiment),8)='GRW_FREE'
			THEN 'free'
		ELSE NULL
		END AS experiment_audience

FROM google_analytics.experiment_cohorts g

UNION

SELECT e.actor_id as member_id,
		JSON_EXTRACT_PATH_TEXT(e.data,'additional_data','id',TRUE) AS experiment,
		JSON_EXTRACT_PATH_TEXT(e.data,'additional_data','variation',TRUE) AS variant,
		DATE_TRUNC('day',e.timestamp) as date,
		CASE WHEN UPPER(experiment) = 'GRW_SS_ACT_1' THEN 'free'
		WHEN (LEFT(UPPER(experiment),3)='EX_' 
		OR LEFT(UPPER(experiment),6)='GRW_SS' 
		OR LEFT(UPPER(experiment),3)='CA_'
		OR LEFT(UPPER(experiment),3)='HP_' 
		OR LEFT(UPPER(experiment),3)='PL_' 
		OR LEFT(LOWER(experiment),9)='sitewide_' 
		OR LEFT(UPPER(experiment),3)='DE_' 
		OR LEFT(UPPER(experiment),4)='ACT_' 
		OR LEFT(UPPER(experiment),4)='ENG_' 
		OR LEFT(UPPER(experiment),3)='PM_' 
		OR LEFT(UPPER(experiment),4)= 'PNC_')
			THEN 'paid'
		WHEN LEFT(UPPER(experiment),8)='GRW_FREE'
			THEN 'free'
		ELSE NULL
		END AS experiment_audience
		
FROM events.event e
WHERE event = 'experiment_started'
AND e.timestamp >= '2021-01-01'

),



members AS (

SELECT DISTINCT ee.member_id,
       ee.experiment,
       MIN (ee.date) AS date
       
FROM experiment_events ee
LEFT JOIN self_serve.customers c ON ee.member_id=c.member_id
LEFT JOIN self_serve.free_to_trial_by_member f ON ee.member_id=f.member_id

WHERE (c.signup_source='web' OR f.signup_source='web')
      AND DATE_TRUNC('year',ee.date)>='2019-01-01'
GROUP BY 1,2  
),

dates AS (

SELECT m.experiment,
       m.date,
       COUNT (DISTINCT member_id) as count
       
FROM members m

GROUP BY 1,2 
),

experiments AS (

SELECT m.experiment, 
       MIN (m.date) AS start_date,
       MAX (d.date) AS end_date
       
FROM members m
LEFT JOIN dates d ON m.experiment=d.experiment

WHERE d.count>=30

GROUP BY 1
),


cohorts AS (

SELECT DISTINCT ee.member_id,
       ee.experiment,
       ee.experiment_audience,
       ee.variant,
       c.acquisition_source,
       me.market,
       e.start_date,
       e.end_date,
       c.trial_plan,
       CASE WHEN c.trial_length IN ('30_days') THEN '30_days' ELSE '14_days' END AS trial_length,
       CASE WHEN me.language IN ('en','es','fr','de') THEN me.language ELSE 'other' END AS language,
       DATE_TRUNC ('day',c.trial_start_date) AS trial_start_date,
       DATE_TRUNC ('day',c.trial_end_date) AS trial_end_date
       

FROM experiment_events ee
INNER JOIN experiments e ON ee.experiment=e.experiment
LEFT JOIN self_serve.customers c ON ee.member_id=c.member_id
LEFT JOIN hootsuite_enhanced.member_enhanced me ON ee.member_id=me.member_id
JOIN mappings.hootsuite_products hp ON hp.legacy_plan_code = c.trial_plan

WHERE ee.experiment_audience = 'paid'
      AND DATE_TRUNC('day',c.trial_start_date) BETWEEN DATE_TRUNC('day',e.start_date) AND DATE_TRUNC('day',e.end_date)
      AND hp.include_in_ecomm = 'TRUE'
      AND hp.is_paid = 'TRUE'
      AND c.trial_type='immediate'
      AND (c.trial_length='30_days' OR (c.trial_length = 'other' AND DATEDIFF('day',c.trial_start_date, c.trial_end_date) = 14))
      --AND me.language = 'en'

UNION ALL

SELECT DISTINCT ee.member_id,
       ee.experiment,
       ee.experiment_audience,
       ee.variant,
       f.acquisition_source,
       me.market,
       e.start_date,
       e.end_date,
       'free' AS trial_plan,
       '0_days' AS trial_length,
       CASE WHEN me.language IN ('en','es','fr','de') THEN me.language ELSE 'other' END AS language,
       f.free_signup_date AS trial_start_date,
       e.end_date AS trial_end_date
       
FROM experiment_events ee
INNER JOIN experiments e ON ee.experiment=e.experiment
LEFT JOIN self_serve.free_to_trial_by_member f ON ee.member_id=f.member_id
LEFT JOIN hootsuite_enhanced.member_enhanced me ON ee.member_id=me.member_id

WHERE ee.experiment_audience = 'free'
      AND f.signup_source='web'
      AND DATE_TRUNC('day',f.free_signup_date) BETWEEN DATE_TRUNC('day',e.start_date) AND DATE_TRUNC('day',e.end_date)
      --AND me.language = 'en'

),

conversions AS (

SELECT DISTINCT c.member_id,
       c.experiment,
       c.experiment_audience,
       c.variant,
       c.acquisition_source,
       c.market,
       CASE WHEN c.experiment_audience = 'paid' THEN c.trial_start_date
            ELSE NULL END AS trial_start_date,
       CASE WHEN c.experiment_audience = 'paid' THEN c.trial_end_date
            ELSE NULL END AS trial_end_date,
       DATE_TRUNC('day',COALESCE(c.trial_start_date,m.date)) AS created_date_c,  
       CASE WHEN sn.owner_member_id IS NOT NULL THEN TRUE
            ELSE FALSE END AS have_sn,
       CASE WHEN tp.converted=1 THEN TRUE
            WHEN tp.member_id IS NULL THEN NULL
            ELSE FALSE END AS converted,
       CASE WHEN DATEDIFF('day',c.trial_start_date,current_date) BETWEEN 0 AND 6 THEN 'Week 1'
            WHEN DATEDIFF('day',c.trial_start_date,current_date) BETWEEN 7 AND 13 THEN 'Week 2'
            WHEN DATEDIFF('day',c.trial_start_date,current_date) BETWEEN 14 AND 20 THEN 'Week 3'
            WHEN DATEDIFF('day',c.trial_start_date,current_date) BETWEEN 21 AND 27 THEN 'Week 4'
            ELSE 'Week 5+' END AS experiment_week,
       c.trial_plan,
       c.trial_length,
       c.language,
       CASE WHEN c.experiment_audience = 'paid' THEN COALESCE(s.setup_moment,FALSE) ELSE COALESCE(sf.setup_moment,FALSE) 
            END AS setup_moment,
       CASE WHEN s.count_social_profiles_added_24h>=1 OR (c.experiment_audience = 'free' AND sf.count_social_profiles_added_24h>=1) THEN TRUE 
            ELSE FALSE END AS h24_added_1_profile,
       CASE WHEN s.count_social_profiles_added_24h>=2 OR (c.experiment_audience = 'free' AND sf.count_social_profiles_added_24h>=2) THEN TRUE 
            ELSE FALSE END AS h24_added_2_profiles,
       CASE WHEN s.count_social_profiles_added_24h>=3 OR (c.experiment_audience = 'free' AND sf.count_social_profiles_added_24h>=3) THEN TRUE 
            ELSE FALSE END AS h24_added_3_profiles,
       CASE WHEN s.count_facebook_pages_added_24h>0 OR (c.experiment_audience = 'free' AND sf.count_facebook_pages_added_24h>0) THEN TRUE 
            ELSE FALSE END AS h24_added_facebook_page,     
       CASE WHEN s.count_igb_direct_publish_added_24h>0 OR (c.experiment_audience = 'free' AND sf.count_igb_direct_publish_added_24h>0) THEN TRUE 
            ELSE FALSE END AS h24_added_igb_direct_publish,     
       CASE WHEN s.count_any_instagram_added_24h>0 OR (c.experiment_audience = 'free' AND sf.count_any_instagram_added_24h>0) THEN TRUE 
            ELSE FALSE END AS h24_added_any_instagram,
       CASE WHEN s.count_twitter_added_24h>0 OR (c.experiment_audience = 'free' AND sf.count_twitter_added_24h>0) THEN TRUE 
            ELSE FALSE END AS h24_added_twitter,
       CASE WHEN c.experiment_audience = 'paid' THEN COALESCE(s.opened_composer_24h,FALSE) 
            ELSE COALESCE(sf.opened_composer_24h,FALSE) 
            END AS h24_opened_composer,
       CASE WHEN c.experiment_audience = 'paid' THEN COALESCE(h.habit_moment,FALSE)
            ELSE COALESCE(hf.habit_moment,FALSE) END AS habit_moment,
       CASE WHEN h.count_messages_published_28d >=5 OR (c.experiment_audience = 'free' AND hf.count_messages_published_28d >=5) THEN TRUE ELSE FALSE END AS d28_5_msgs_published,
       CASE WHEN h.count_streams_scrolls_28d >=20 OR (c.experiment_audience = 'free' AND hf.count_streams_scrolls_28d >=20) THEN TRUE ELSE FALSE END AS d28_20_scrolls,
       CASE WHEN h.count_edits_planner_28d >=5 OR (c.experiment_audience = 'free' AND hf.count_edits_planner_28d >=5) THEN TRUE ELSE FALSE END AS d28_5_edits,
       CASE WHEN d28_5_msgs_published IS TRUE AND d28_20_scrolls IS TRUE THEN TRUE ELSE FALSE END AS d28_5_published_20_scrolled,
       CASE WHEN d28_5_msgs_published IS TRUE AND d28_5_edits IS TRUE THEN TRUE ELSE FALSE END AS d28_5_published_5_edited,
       CASE WHEN c.experiment_audience = 'paid' THEN COALESCE(aha.aha_moment,FALSE) ELSE COALESCE(ahaf.aha_moment,FALSE) 
       END AS aha_moment,
       CASE WHEN aha.count_messages_published_5d >0 OR (c.experiment_audience = 'free' AND ahaf.count_messages_published_5d >0) THEN TRUE ELSE FALSE END AS d5_published,
       CASE WHEN aha.count_streams_scrolls_5d >0 OR (c.experiment_audience = 'free' AND ahaf.count_streams_scrolls_5d >0) THEN TRUE ELSE FALSE END AS d5_scrolled,
       CASE WHEN aha.count_edits_planner_5d >0 OR (c.experiment_audience = 'free' AND ahaf.count_edits_planner_5d >0) THEN TRUE ELSE FALSE END AS d5_edited_planner,
       CASE WHEN d5_published IS TRUE AND d5_scrolled IS TRUE THEN TRUE ELSE FALSE END AS d5_published_and_scrolled,
       CASE WHEN d5_published IS TRUE AND d5_edited_planner IS TRUE THEN TRUE ELSE FALSE END AS d5_published_and_edited
        
FROM cohorts c
LEFT JOIN hootsuite_enhanced.social_network_enhanced sn ON c.member_id=sn.owner_member_id
LEFT JOIN self_serve.trial_to_paying_by_member tp ON c.member_id=tp.member_id
LEFT JOIN members m ON c.member_id=m.member_id AND c.experiment=m.experiment
LEFT JOIN product.setup_self_serve_by_member s ON c.member_id=s.member_id
LEFT JOIN product.setup_free_by_member sf ON c.member_id=sf.member_id
LEFT JOIN product.habit_self_serve_by_member h ON c.member_id=h.member_id
LEFT JOIN product.habit_free_by_member hf ON c.member_id=hf.member_id
LEFT JOIN product.aha_self_serve_by_member aha ON c.member_id=aha.member_id
LEFT JOIN product.aha_free_by_member ahaf ON c.member_id=ahaf.member_id


WHERE (c.experiment_audience = 'paid' AND tp.trial_length='30_days') 
OR c.experiment_audience = 'free'
--takes only 30 days trials that we consider as trials for T:P
OR (c.experiment_audience = 'paid' AND c.trial_length IN ('14_days', '30_days'))
),


refunds AS (
 
SELECT DISTINCT ip.member_id,
       MIN (ip.payment_date) AS payment_date
	

FROM self_serve.invoice_payments ip
INNER JOIN conversions c ON ip.member_id=c.member_id
LEFT JOIN self_serve.first_payments fp ON ip.member_id = fp.member_id

WHERE ip.amount_in_usd <0
	AND ip.plan_name NOT LIKE '%Prorated%'
	AND DATE_TRUNC('day',ip.payment_date) BETWEEN  DATE_TRUNC('day',c.trial_start_date) AND DATE_TRUNC('day',fp.first_overall_payment)+59. --customers can be refunded during 60 days after the payment
	AND c.experiment_audience = 'paid'
	
GROUP BY 1
),

finish_step_2 AS (

SELECT DISTINCT c.member_id,
       c.created_date_c,
       MIN (e.timestamp) AS date

FROM conversions c 
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('day', 29, c.created_date_c)


WHERE e.event IN ('finish_connecting_sn','user_clicks_continue_button','account_setup_user_click_skip_button','user_clicks_add_more_later')

GROUP BY 1,2
--HAVING DATEDIFF('day',c.created_date_c,MIN (e.timestamp)) BETWEEN 0 AND 29
),

enter_dashboard AS (

SELECT DISTINCT c.member_id,
       c.created_date_c,
       MIN (e.timestamp) AS date

FROM conversions c 
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('day', 29, c.created_date_c)

WHERE e.event='web.dashboard.load'

GROUP BY 1,2
--HAVING DATEDIFF('day',c.created_date_c,MIN (e.timestamp)) BETWEEN 0 AND 29
),

step_2_sns_added AS (

SELECT c.member_id,
       COUNT (DISTINCT e.social_network_id) AS sns_added

FROM conversions c
LEFT JOIN enter_dashboard ed ON ed.member_id=c.member_id
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('day', 29, c.created_date_c)


WHERE (DATEDIFF('second',e.timestamp,ed.date)>0 OR ed.date IS NULL)---SNs added before the first enter to the dashboard
      AND (LEFT(e.event, 19) = 'add_social_network_' OR RIGHT(e.event,14) = '_steal_success')
      --AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 0 AND 29
      
GROUP BY 1
),

d0_actions AS (

SELECT DISTINCT c.member_id,
       SUM (CASE WHEN e.event IN ('instagram_steal_start','instagram_add_start','instagrambusiness_add_start','instagrambusiness_steal_start','instagram_reauth_start','instagrambusiness_reauth_start','user_clicks_add_sn_button_instagram') THEN 1 ELSE 0 END) AS count_add_instagram_attempts_0d,
       CASE WHEN count_add_instagram_attempts_0d>0 THEN TRUE ELSE FALSE END AS d0_attempted_add_instagram,
       SUM(CASE WHEN e.event IN ('content_planner_opened', 'dashboard_planner_clicked') THEN 1 ELSE 0 END) AS count_h24_planner_navigations_0d,
       SUM(CASE WHEN e.event IN ('analytics_app_loaded', 'dashboard_new_analytics_clicked') THEN 1 ELSE 0 END) AS count_analytics_navigations_0d,
       SUM(CASE WHEN e.event IN ('dashboard_streams_clicked') THEN 1 ELSE 0 END) AS count_streams_navigations_0d,
       
       CASE WHEN count_h24_planner_navigations_0d>0 THEN TRUE ELSE FALSE END AS d0_navigated_planner,
       CASE WHEN count_analytics_navigations_0d>0 THEN TRUE ELSE FALSE END AS d0_navigated_analytics,
       CASE WHEN count_streams_navigations_0d>0 THEN TRUE ELSE FALSE END AS d0_navigated_streams

FROM conversions c
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp < DATEADD('day', 1, c.created_date_c)


WHERE e.event IN ('instagram_steal_start','instagram_add_start','instagrambusiness_add_start','instagrambusiness_steal_start','instagram_reauth_start','instagrambusiness_reauth_start','user_clicks_add_sn_button_instagram','content_planner_opened','dashboard_planner_clicked','analytics_app_loaded','dashboard_new_analytics_clicked','dashboard_streams_clicked')
      --AND DATEDIFF('day',c.created_date_c,e.timestamp)=0
      
GROUP BY 1
),

d5_actions AS (

SELECT DISTINCT c.member_id,
       SUM (CASE WHEN e.event='dashboard_publisher_clicked' THEN 1 ELSE 0 END) AS count_d5_publisher_navigations,
       SUM (CASE WHEN e.event IN ('new_post_clicked','compose_new_message_from_source_page') AND JSON_EXTRACT_PATH_TEXT(e.data,'additional_data','sourcePage',TRUE)='planner' THEN 1 ELSE 0 END) AS count_d5_post_ctas_planner,
       SUM (CASE WHEN e.event IN ('compose_new_message_from_source_page','compose_new_message','open_new_compose_from_template','new_post_clicked','new_pin_clicked','new_story_clicked','stream_user_clicked_schedule_cta','user_clicks_new_post','open_new_compose','compose_new_message_for_network','open_new_compose_instagramstory','open_new_compose_pinterest','open_legacy_compose') THEN 1 ELSE 0 END) AS count_d5_opened_composer,
       SUM(CASE WHEN e.event IN ('content_planner_opened', 'dashboard_planner_clicked') THEN 1 ELSE 0 END) AS count_d5_planner_navigations,
       SUM(CASE WHEN e.event IN ('analytics_app_loaded', 'dashboard_new_analytics_clicked') THEN 1 ELSE 0 END) AS count_d5_analytics_navigations,
       SUM(CASE WHEN e.event IN ('dashboard_streams_clicked') THEN 1 ELSE 0 END) AS count_d5_streams_navigations,
       
       CASE WHEN count_d5_publisher_navigations>0 THEN TRUE ELSE FALSE END AS d5_navigated_publisher,
       CASE WHEN count_d5_post_ctas_planner>0 THEN TRUE ELSE FALSE END AS d5_clicked_post_ctas_planner,
       CASE WHEN count_d5_opened_composer>0 THEN TRUE ELSE FALSE END AS d5_opened_composer,
       CASE WHEN count_d5_planner_navigations>0 THEN TRUE ELSE FALSE END AS d5_navigated_planner,
       CASE WHEN count_d5_analytics_navigations>0 THEN TRUE ELSE FALSE END AS d5_navigated_analytics,
       CASE WHEN count_d5_streams_navigations>0 THEN TRUE ELSE FALSE END AS d5_navigated_streams


FROM conversions c
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('hour', 119, c.created_date_c)


WHERE e.event IN ('dashboard_publisher_clicked','compose_new_message_from_source_page','compose_new_message','open_new_compose_from_template','new_post_clicked','new_pin_clicked','new_story_clicked','stream_user_clicked_schedule_cta','user_clicks_new_post','open_new_compose','compose_new_message_for_network','open_new_compose_instagramstory','open_new_compose_pinterest','open_legacy_compose','content_planner_opened','dashboard_planner_clicked','analytics_app_loaded','dashboard_new_analytics_clicked','dashboard_streams_clicked') 
      --AND DATEDIFF('hour',c.created_date_c,e.timestamp) BETWEEN 0 AND 119
      
GROUP BY 1
),

d28_actions AS (

SELECT DISTINCT c.member_id,
       SUM (CASE WHEN e.event='add_team_member' THEN 1 ELSE 0 END) AS count_d28_team_members_added,
       
       SUM (CASE WHEN e.event='dashboard_publisher_clicked' THEN 1 ELSE 0 END) AS count_d28_publisher_navigations,
       SUM (CASE WHEN e.event IN ('compose_new_message_from_source_page','compose_new_message','open_new_compose_from_template','new_post_clicked','new_pin_clicked','new_story_clicked','stream_user_clicked_schedule_cta','user_clicks_new_post','open_new_compose','compose_new_message_for_network','open_new_compose_instagramstory','open_new_compose_pinterest','open_legacy_compose') THEN 1 ELSE 0 END) AS count_d28_opened_composer,
       SUM(CASE WHEN e.event IN ('content_planner_opened', 'dashboard_planner_clicked') THEN 1 ELSE 0 END) AS count_d28_planner_navigations,
       SUM(CASE WHEN e.event IN ('analytics_app_loaded', 'dashboard_new_analytics_clicked') THEN 1 ELSE 0 END) AS count_d28_analytics_navigations,
       SUM(CASE WHEN e.event IN ('dashboard_streams_clicked') THEN 1 ELSE 0 END) AS count_d28_streams_navigations,

       CASE WHEN count_d28_team_members_added>0 THEN TRUE ELSE FALSE END AS d28_added_team_member, 
       CASE WHEN count_d28_publisher_navigations>0 THEN TRUE ELSE FALSE END AS d28_navigated_publisher,
       CASE WHEN count_d28_opened_composer>0 THEN TRUE ELSE FALSE END AS d28_opened_composer,
       CASE WHEN count_d28_planner_navigations>0 THEN TRUE ELSE FALSE END AS d28_navigated_planner,
       CASE WHEN count_d28_analytics_navigations>0 THEN TRUE ELSE FALSE END AS d28_navigated_analytics,
       CASE WHEN count_d28_streams_navigations>0 THEN TRUE ELSE FALSE END AS d28_navigated_streams
FROM conversions c
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('day', 27, c.created_date_c)


WHERE e.event IN ('add_team_member','content_planner_opened','dashboard_planner_clicked','analytics_app_loaded','dashboard_new_analytics_clicked','dashboard_streams_clicked')
      --AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 0 AND 27
      
GROUP BY 1
),

engagement_sp AS (---requires f_event_social_network_id custom function set up

SELECT DISTINCT c.member_id,
       COUNT (DISTINCT e.social_network_id) AS count_sp_engaged

FROM conversions c 
INNER JOIN events.event e ON c.member_id=e.actor_id
AND e.timestamp >= c.created_date_c and timestamp <= DATEADD('day', 29, c.created_date_c)

WHERE e.event NOT IN ('drop_stream','add_stream','message_send_failed') 
      AND LEFT(e.event,18)<>'add_social_network' AND LEFT(e.event,19)<>'drop_social_network' AND RIGHT(e.event,14) <> '_steal_success'
      --AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 0 AND 29

GROUP BY 1
), 

m1_m3_retention AS (

SELECT DISTINCT c.member_id,
       c.experiment,
       c.experiment_audience,
       SUM (CASE WHEN c.experiment_audience = 'paid' AND hp.is_paid='True' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 30 AND 59 THEN 1 ELSE 0 END) AS m1_paid,      
       SUM (CASE WHEN c.experiment_audience = 'paid' AND hp.is_paid='True' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 60 AND 89 THEN 1 ELSE 0 END) AS m2_paid,       
       SUM (CASE WHEN c.experiment_audience = 'paid' AND hp.is_paid='True' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 90 AND 119 THEN 1 ELSE 0 END) AS m3_paid,      
       SUM (CASE WHEN c.experiment_audience = 'free' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 30 AND 59 THEN 1 ELSE 0 END) AS m1_free,      
       SUM (CASE WHEN c.experiment_audience = 'free' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 60 AND 89 THEN 1 ELSE 0 END) AS m2_free,   
       SUM (CASE WHEN c.experiment_audience = 'free' AND DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 90 AND 119 THEN 1 ELSE 0 END) AS m3_free,
       CASE WHEN (c.experiment_audience = 'paid' AND m1_paid>0) OR (c.experiment_audience = 'free' AND m1_free>0) THEN TRUE ELSE FALSE END AS m1_retained,    
       CASE WHEN (c.experiment_audience = 'paid' AND m2_paid>0) OR (c.experiment_audience = 'free' AND m2_free>0) THEN TRUE ELSE FALSE END AS m2_retained,     
       CASE WHEN (c.experiment_audience = 'paid' AND m3_paid>0) OR (c.experiment_audience = 'free' AND m3_free>0) THEN TRUE ELSE FALSE END AS m3_retained
           
FROM conversions c
INNER JOIN events.event e ON c.member_id=e.actor_id
	AND e.event IN (SELECT DISTINCT a.event FROM product.meaningful_activity a)
	AND e.timestamp >= DATEADD('day', 30, c.created_date_c) and timestamp <= DATEADD('day', 119, c.created_date_c)

JOIN mappings.hootsuite_products hp ON hp.billing_product_code = e.member_effective_product_code 

WHERE (hp.include_in_ecomm ='TRUE' OR hp.include_in_enterprise ='TRUE') OR (e.member_max_plan IS NULL OR e.member_effective_product_code IS NULL)


 --3 months after finishing 30d trial

--WHERE DATEDIFF('day',c.created_date_c,e.timestamp) BETWEEN 30 AND 119
      
GROUP BY 1,2,3
),

activity AS (

SELECT DISTINCT c.member_id,
       COUNT(DISTINCT a.activity_date_utc) AS days_active

FROM conversions c
LEFT JOIN product.engagement_actions_daily_by_member a ON c.member_id=a.member_id
AND a.activity_date_utc >= c.created_date_c and a.activity_date_utc <= DATEADD('day', 29, c.created_date_c)


--WHERE DATEDIFF('day',c.created_date_c,a.activity_date_utc) BETWEEN 0 AND 29
    
GROUP BY 1
),



summary AS (
            
SELECT DISTINCT c.member_id,
       c.experiment,
       c.variant,      
       c.acquisition_source,
       c.market,
       c.created_date_c AS created_date,    
       c.have_sn,
       c.converted,
       COALESCE(a.days_active,0) AS regularity,
       CASE WHEN DATEDIFF('day',c.trial_end_date,current_date)>0 AND c.trial_end_date IS NOT NULL THEN TRUE
            WHEN c.trial_end_date IS NULL THEN NULL
            ELSE FALSE END AS finished_trial,
       CASE WHEN f.member_id IS NOT NULL THEN TRUE
            ELSE FALSE END AS finished_step2,
       CASE WHEN ed.member_id IS NOT NULL AND DATEDIFF('day',ed.created_date_c,ed.date)=0 THEN TRUE
            ELSE FALSE END AS d0_entered_dashboard,
       COALESCE(s2.sns_added,0) AS step2_sns_added,
       c.experiment_week,
       COALESCE(esp.count_sp_engaged,0) AS count_sp_engaged,    
       c.trial_plan,
       c.trial_length,
       c.language,
       COALESCE(r.m1_retained,FALSE) AS m1_retained,
       COALESCE(r.m2_retained,FALSE) AS m2_retained,
       COALESCE(r.m3_retained,FALSE) AS m3_retained,
       c.habit_moment,
       c.d28_5_msgs_published,
       c.d28_20_scrolls,
       c.d28_5_edits,
       c.d28_5_published_20_scrolled,
       c.d28_5_published_5_edited,
       CASE WHEN ref.member_id THEN TRUE
            ELSE FALSE END AS refunded,
       c.aha_moment,
       c.d5_published,
       c.d5_scrolled,
       c.d5_edited_planner,
       c.d5_published_and_scrolled,
       c.d5_published_and_edited,

       COALESCE(d0.d0_navigated_planner,FALSE) AS h24_navigated_planner,
       COALESCE(d0.d0_navigated_analytics,FALSE) AS h24_navigated_analytics, 
       COALESCE(d0.d0_navigated_streams,FALSE) AS h24_navigated_streams, 

       COALESCE(d5.d5_navigated_publisher,FALSE) AS d5_navigated_publisher,
       COALESCE(d5.d5_clicked_post_ctas_planner,FALSE) AS d5_clicked_post_ctas_planner,
       COALESCE(d5.d5_opened_composer,FALSE) AS d5_opened_composer,
       COALESCE(d5.d5_navigated_planner,FALSE) AS d5_navigated_planner,
       COALESCE(d5.d5_navigated_analytics,FALSE) AS d5_navigated_analytics, 
       COALESCE(d5.d5_navigated_streams,FALSE) AS d5_navigated_streams, 
       
       COALESCE(d28.d28_navigated_publisher,FALSE) AS d28_navigated_publisher,
       COALESCE(d28.d28_navigated_planner,FALSE) AS d28_navigated_planner,
       COALESCE(d28.d28_opened_composer,FALSE) AS d28_opened_composer,
       COALESCE(d28.d28_navigated_analytics,FALSE) AS d28_navigated_analytics,
       COALESCE(d28.d28_navigated_streams,FALSE) AS d28_navigated_streams,
       COALESCE(d28.d28_added_team_member,FALSE) AS d28_added_team_member,
       
       COALESCE(d0.d0_attempted_add_instagram,FALSE) AS h24_attempted_add_instagram,
       c.setup_moment,
       c.h24_added_1_profile,
       c.h24_added_2_profiles,
       c.h24_added_3_profiles,
       c.h24_added_facebook_page,     
       c.h24_added_igb_direct_publish,     
       c.h24_added_any_instagram,
       c.h24_added_twitter,
       c.h24_opened_composer,
       CASE WHEN c.experiment = 'GRW_SS_OB_2_2' AND c.created_date_c >= '2022-04-23' THEN TRUE ELSE FALSE END AS exclude_ind
       
FROM conversions c
LEFT JOIN activity a ON c.member_id=a.member_id
LEFT JOIN finish_step_2 f ON c.member_id=f.member_id
LEFT JOIN enter_dashboard ed ON c.member_id=ed.member_id
LEFT JOIN step_2_sns_added s2 ON c.member_id=s2.member_id
LEFT JOIN engagement_sp esp ON c.member_id=esp.member_id
LEFT JOIN m1_m3_retention r ON c.member_id=r.member_id AND c.experiment=r.experiment
LEFT JOIN refunds ref ON c.member_id=ref.member_id
LEFT JOIN d5_actions d5 ON c.member_id=d5.member_id
LEFT JOIN d28_actions d28 ON c.member_id=d28.member_id
LEFT JOIN d0_actions d0 ON c.member_id=d0.member_id
WHERE exclude_ind = FALSE


)      


SELECT DISTINCT s.experiment,
       s.variant,    
       s.acquisition_source,
       s.market,
       s.created_date,   
       s.have_sn,
       s.converted,
       s.regularity,
       s.finished_trial,
       s.finished_step2,
       s.d0_entered_dashboard,
       s.step2_sns_added,
       s.experiment_week,
       s.count_sp_engaged,
       s.trial_plan,
       s.trial_length,
       s.language,
       s.m1_retained,
       s.m2_retained,
       s.m3_retained,
       s.habit_moment,
       s.d28_5_msgs_published,
       s.d28_20_scrolls,
       s.d28_5_edits,
       s.d28_5_published_20_scrolled,
       s.d28_5_published_5_edited,
       s.refunded,
       s.aha_moment,
       s.d5_published,
       s.d5_scrolled,
       s.d5_edited_planner,
       s.d5_published_and_scrolled,
       s.d5_published_and_edited,
       s.d5_navigated_publisher,
       s.d5_clicked_post_ctas_planner,
       s.d5_opened_composer,
       s.d5_navigated_planner,
       s.d5_navigated_analytics,
       s.d5_navigated_streams,
       s.d28_navigated_publisher,
       s.d28_navigated_planner,
       s.d28_opened_composer,
       s.d28_navigated_analytics,
       s.d28_navigated_streams,
       s.d28_added_team_member,
       s.h24_attempted_add_instagram,
       s.setup_moment,
       s.h24_added_1_profile,
       s.h24_added_2_profiles,
       s.h24_added_3_profiles,
       s.h24_added_facebook_page,     
       s.h24_added_igb_direct_publish,     
       s.h24_added_any_instagram,
       s.h24_added_twitter,
       s.h24_opened_composer,
       s.h24_navigated_planner,
       s.h24_navigated_analytics,
       s.h24_navigated_streams,
       COUNT (DISTINCT s.member_id) AS count_members

FROM summary s

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58 
)
;
