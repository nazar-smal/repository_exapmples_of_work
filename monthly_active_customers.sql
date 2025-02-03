WITH
meaningful_activity_cte AS (
  SELECT * FROM {{ ref('meaningful_activity')}}
),

self_serve_customers_cte AS (
  SELECT * FROM {{ ref('customers')}}
),

hootsuite_organization_cte AS (
  SELECT * FROM {{ ref('hootsuite__organization')}}
),

events_cte AS (
  SELECT DISTINCT a.activity_category,
  e.actor_id AS member_id,
  DATE_TRUNC ('month',e.timestamp) AS month_of_activity,
  e.timestamp,
  CASE WHEN e.event NOT ILIKE '%_messages_summary' THEN e.social_network_type ELSE NULL END AS social_network_type, --remove social network type counts associated with _messages_summary that would cause count inflation
  LAST_VALUE(e.organization_id) OVER(PARTITION BY e.actor_id, month_of_activity ORDER BY e.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS organization_id

  FROM {{ ref('events__event')}} e
  INNER JOIN meaningful_activity_cte a ON e.event = a.event AND is_active_event = TRUE
  WHERE DATE_TRUNC('year',e.timestamp) >= '2018-01-01'
  AND e.member_effective_product_code <> 'EMPLOYEE'
  {% if is_incremental() %}
  AND e.timestamp >= DATE_TRUNC('month','{{get_data_interval_end()}}'::date) - interval '2 months'
  {% endif %}
),

last_organization_id_cte AS (
  SELECT DISTINCT v.activity_category,
  v.member_id,
  COALESCE(c1.member_id,c2.member_id) AS payment_member_id,
  LAST_VALUE(v.organization_id) IGNORE NULLS OVER(PARTITION BY COALESCE(c1.member_id,c2.member_id,v.member_id), v.month_of_activity ORDER BY v.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS organization_id,
  v.month_of_activity,
  v.timestamp,
  v.social_network_type

  FROM events_cte v
  LEFT JOIN hootsuite_organization_cte o ON v.organization_id = o.organization_id
  LEFT JOIN self_serve_customers_cte c1 ON o.payment_member_id = c1.member_id
  LEFT JOIN self_serve_customers_cte c2 ON v.member_id = c2.member_id
),

organization_not_null_cte AS (
  SELECT DISTINCT v.activity_category,
  v.member_id,
  LAST_VALUE(v.payment_member_id) IGNORE NULLS OVER(PARTITION BY COALESCE(v.organization_id), v.month_of_activity ORDER BY v.timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS payment_member_id,
  v.organization_id,
  v.month_of_activity,
  v.timestamp,
  v.social_network_type

  FROM last_organization_id_cte v
  WHERE v.organization_id IS NOT NULL
),

organization_is_null_cte AS (
  SELECT DISTINCT v.activity_category,
  v.member_id,
  NULL AS payment_member_id,
  v.organization_id,
  v.month_of_activity,
  v.timestamp,
  v.social_network_type

  FROM last_organization_id_cte v
  WHERE v.organization_id IS NULL
),

organization_events_count_cte AS (
  SELECT v.organization_id,
  payment_member_id AS member_id,
  v.month_of_activity,
  COUNT(v.timestamp) AS count_active_events,
  COUNT(CASE WHEN v.activity_category = 'composer' THEN v.timestamp END) AS count_composer_events,
  COUNT(CASE WHEN v.activity_category = 'planner' THEN v.timestamp END) AS count_planner_events,
  COUNT(CASE WHEN v.activity_category = 'analytics' THEN v.timestamp END) AS count_analytics_events,
  COUNT(CASE WHEN v.activity_category = 'amplify' THEN v.timestamp END) AS count_amplify_events,
  COUNT(CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.timestamp END) AS count_any_inbox_events,
  COUNT(CASE WHEN v.activity_category = 'inbox_1' THEN v.timestamp END) AS count_inbox_1_events,
  COUNT(CASE WHEN v.activity_category = 'inbox_2' THEN v.timestamp END) AS count_inbox_2_events,
  COUNT(CASE WHEN v.activity_category = 'streams' THEN v.timestamp END) AS count_streams_events,

  COUNT(DISTINCT v.timestamp::date) AS days_active,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'composer' THEN v.timestamp::date END) AS days_active_composer,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'planner' THEN v.timestamp::date END) AS days_active_planner,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'analytics' THEN v.timestamp::date END) AS days_active_analytics,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'amplify' THEN v.timestamp::date END) AS days_active_amplify,
  COUNT(DISTINCT CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.timestamp::date END) AS days_active_any_inbox,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_1' THEN v.timestamp::date END) AS days_active_inbox_1,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_2' THEN v.timestamp::date END) AS days_active_inbox_2,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'streams' THEN v.timestamp::date END) AS days_active_streams,

  COUNT(DISTINCT v.member_id) AS count_active_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'composer' THEN v.member_id END) AS count_active_composer_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'planner' THEN v.member_id END) AS count_active_planner_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'analytics' THEN v.member_id END) AS count_active_analytics_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'amplify' THEN v.member_id END) AS count_active_amplify_users,
  COUNT(DISTINCT CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.member_id END) AS count_active_any_inbox_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_1' THEN v.member_id END) AS count_active_inbox_1_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_2' THEN v.member_id END) AS count_active_inbox_2_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'streams' THEN v.member_id END) AS count_active_streams_users,

  COUNT(DISTINCT v.social_network_type) AS count_social_networks_used,
  COUNT(DISTINCT v.activity_category) AS count_products

  FROM organization_not_null_cte v
  GROUP BY 1,2,3
),

member_events_count_cte AS (
  SELECT NULL::bigint AS organization_id,
  v.member_id,
  v.month_of_activity,
  COUNT(v.timestamp) AS count_active_events,
  COUNT(CASE WHEN v.activity_category = 'composer' THEN v.timestamp END) AS count_composer_events,
  COUNT(CASE WHEN v.activity_category = 'planner' THEN v.timestamp END) AS count_planner_events,
  COUNT(CASE WHEN v.activity_category = 'analytics' THEN v.timestamp END) AS count_analytics_events,
  COUNT(CASE WHEN v.activity_category = 'amplify' THEN v.timestamp END) AS count_amplify_events,
  COUNT(CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.timestamp END) AS count_any_inbox_events,
  COUNT(CASE WHEN v.activity_category = 'inbox_1' THEN v.timestamp END) AS count_inbox_1_events,
  COUNT(CASE WHEN v.activity_category = 'inbox_2' THEN v.timestamp END) AS count_inbox_2_events,
  COUNT(CASE WHEN v.activity_category = 'streams' THEN v.timestamp END) AS count_streams_events,

  COUNT(DISTINCT v.timestamp::date) AS days_active,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'composer' THEN v.timestamp::date END) AS days_active_composer,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'planner' THEN v.timestamp::date END) AS days_active_planner,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'analytics' THEN v.timestamp::date END) AS days_active_analytics,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'amplify' THEN v.timestamp::date END) AS days_active_amplify,
  COUNT(DISTINCT CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.timestamp::date END) AS days_active_any_inbox,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_1' THEN v.timestamp::date END) AS days_active_inbox_1,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_2' THEN v.timestamp::date END) AS days_active_inbox_2,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'streams' THEN v.timestamp::date END) AS days_active_streams,

  COUNT(DISTINCT v.member_id) AS count_active_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'composer' THEN v.member_id END) AS count_active_composer_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'planner' THEN v.member_id END) AS count_active_planner_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'analytics' THEN v.member_id END) AS count_active_analytics_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'amplify' THEN v.member_id END) AS count_active_amplify_users,
  COUNT(DISTINCT CASE WHEN v.activity_category IN ('inbox_1','inbox_2') THEN v.member_id END) AS count_active_any_inbox_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_1' THEN v.member_id END) AS count_active_inbox_1_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'inbox_2' THEN v.member_id END) AS count_active_inbox_2_users,
  COUNT(DISTINCT CASE WHEN v.activity_category = 'streams' THEN v.member_id END) AS count_active_streams_users,

  COUNT(DISTINCT v.social_network_type) AS count_social_networks_used,
  COUNT(DISTINCT v.activity_category) AS count_products

  FROM organization_is_null_cte v
  GROUP BY 1,2,3
),


organization_final_events_cte AS (
  SELECT organization_id,
  member_id,
  month_of_activity,
  CASE WHEN count_active_events > 0 THEN TRUE ELSE FALSE END AS is_active,
  CASE WHEN count_composer_events > 0 THEN TRUE ELSE FALSE END AS is_active_composer,
  CASE WHEN count_planner_events > 0 THEN TRUE ELSE FALSE END AS is_active_planner,
  CASE WHEN count_analytics_events > 0 THEN TRUE ELSE FALSE END AS is_active_analytics,
  CASE WHEN count_amplify_events > 0 THEN TRUE ELSE FALSE END AS is_active_amplify,
  CASE WHEN count_any_inbox_events > 0 THEN TRUE ELSE FALSE END AS is_active_any_inbox,  
  CASE WHEN count_inbox_1_events > 0 THEN TRUE ELSE FALSE END AS is_active_inbox_1,
  CASE WHEN count_inbox_2_events > 0 THEN TRUE ELSE FALSE END AS is_active_inbox_2,
  CASE WHEN count_streams_events > 0 THEN TRUE ELSE FALSE END AS is_active_streams,
  count_active_events,
  count_composer_events,
  count_planner_events,
  count_analytics_events,
  count_amplify_events,
  count_any_inbox_events,
  count_inbox_1_events,
  count_inbox_2_events,
  count_streams_events,
  count_active_users,
  count_active_composer_users,
  count_active_planner_users,
  count_active_analytics_users,
  count_active_amplify_users,
  count_active_any_inbox_users,
  count_active_inbox_1_users,
  count_active_inbox_2_users,
  count_active_streams_users,
  days_active,
  days_active_composer,
  days_active_planner,
  days_active_analytics,
  days_active_amplify,
  days_active_any_inbox,
  days_active_inbox_1,
  days_active_inbox_2,
  days_active_streams,
  count_social_networks_used,
  count_products

  FROM organization_events_count_cte
),

member_final_events_cte AS (
  SELECT organization_id,
  member_id,
  month_of_activity,
  CASE WHEN count_active_events > 0 THEN TRUE ELSE FALSE END AS is_active,
  CASE WHEN count_composer_events > 0 THEN TRUE ELSE FALSE END AS is_active_composer,
  CASE WHEN count_planner_events > 0 THEN TRUE ELSE FALSE END AS is_active_planner,
  CASE WHEN count_analytics_events > 0 THEN TRUE ELSE FALSE END AS is_active_analytics,
  CASE WHEN count_amplify_events > 0 THEN TRUE ELSE FALSE END AS is_active_amplify,
  CASE WHEN count_any_inbox_events > 0 THEN TRUE ELSE FALSE END AS is_active_any_inbox,  
  CASE WHEN count_inbox_1_events > 0 THEN TRUE ELSE FALSE END AS is_active_inbox_1,
  CASE WHEN count_inbox_2_events > 0 THEN TRUE ELSE FALSE END AS is_active_inbox_2,
  CASE WHEN count_streams_events > 0 THEN TRUE ELSE FALSE END AS is_active_streams,
  count_active_events,
  count_composer_events,
  count_planner_events,
  count_analytics_events,
  count_amplify_events,
  count_any_inbox_events,
  count_inbox_1_events,
  count_inbox_2_events,
  count_streams_events,
  count_active_users,
  count_active_composer_users,
  count_active_planner_users,
  count_active_analytics_users,
  count_active_amplify_users,
  count_active_any_inbox_users,
  count_active_inbox_1_users,
  count_active_inbox_2_users,
  count_active_streams_users,
  days_active,
  days_active_composer,
  days_active_planner,
  days_active_analytics,
  days_active_amplify,
  days_active_any_inbox,
  days_active_inbox_1,
  days_active_inbox_2,
  days_active_streams,
  count_social_networks_used,
  count_products

  FROM member_events_count_cte
),

final_cte AS (
  SELECT * FROM organization_final_events_cte

  UNION ALL

  SELECT * FROM member_final_events_cte
)


  SELECT {{ dbt_utils.generate_surrogate_key(['member_id', 'organization_id', 'month_of_activity']) }} AS primary_key,
  member_id,
  organization_id,
  month_of_activity,
  is_active,
  is_active_composer,
  is_active_planner,
  is_active_analytics,
  is_active_amplify,
  is_active_any_inbox,
  is_active_inbox_1,
  is_active_inbox_2,
  is_active_streams,
  count_active_events,
  count_composer_events,
  count_planner_events,
  count_analytics_events,
  count_amplify_events,
  count_any_inbox_events,
  count_inbox_1_events,
  count_inbox_2_events,
  count_streams_events,
  count_active_users,
  count_active_composer_users,
  count_active_planner_users,
  count_active_analytics_users,
  count_active_amplify_users,
  count_active_any_inbox_users,
  count_active_inbox_1_users,
  count_active_inbox_2_users,
  count_active_streams_users,
  days_active,
  days_active_composer,
  days_active_planner,
  days_active_analytics,
  days_active_amplify,
  days_active_any_inbox,
  days_active_inbox_1,
  days_active_inbox_2,
  days_active_streams,
  count_social_networks_used,
  count_products

  FROM final_cte