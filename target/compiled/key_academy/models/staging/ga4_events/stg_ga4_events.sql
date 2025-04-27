

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

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`