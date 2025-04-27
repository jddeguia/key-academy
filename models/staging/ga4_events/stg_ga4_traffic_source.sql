{{
  config(
    materialized = 'table',
    description = 'Traffic source data with event-level timestamps for key events.',
    partition_by = {'field': 'event_date', 'data_type': 'date'},
    cluster_by = ['user_id', 'event_name']
  )
}}

SELECT
  -- User identifiers
  user_pseudo_id,
  COALESCE(user_id, user_pseudo_id) AS user_id,
  
  -- Event info
  event_name,
  event_timestamp,
  user_first_touch_timestamp,
  event_date,
  
  -- Standard Traffic Source
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source,
  
  -- Collected Traffic Source (UTM parameters)
  collected_traffic_source_manual_campaign_id AS campaign_id,
  collected_traffic_source_manual_campaign_name AS campaign_name,
  collected_traffic_source_manual_source AS utm_source,
  collected_traffic_source_manual_medium AS utm_medium,
  collected_traffic_source_manual_term AS utm_term,
  collected_traffic_source_manual_content AS utm_content,
  
  -- Campaign IDs
  collected_traffic_source_gclid AS gclid,
  collected_traffic_source_dclid AS dclid,
  
  -- Device/geo context
  device_category,
  device_operating_system,
  country,
  region,
  city,
  
  -- Session info
  stream_id,
  platform

FROM {{ ref('base_ga4_events') }}
WHERE event_name IN (
  'first_visit',
  'session_start',
  'page_view',
  'purchase',
  'begin_checkout',
  'sign_up'
)