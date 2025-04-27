{{
  config(
    materialized = 'table',
    description = 'Clean staging model of GA4 events with essential fields for analysis. Partitioned by event_date, clustered by event_name.',
    partition_by = {
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by = ["event_name", "user_id"]
  )
}}

SELECT
  -- Event identifiers
  event_name,
  event_timestamp,
  event_previous_timestamp,
  user_first_touch_timestamp,
  event_date,
  
  -- User identifiers
  COALESCE(user_id, user_pseudo_id) AS user_id,
  user_pseudo_id,
  
  -- Device info
  device_category,
  device_operating_system,
  device_operating_system_version,
  device_browser,
  
  -- Location info
  country,
  region,
  city,
  
  -- Traffic source
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source,
  
  -- Session info
  stream_id,
  platform

FROM {{ ref('base_ga4_events') }}