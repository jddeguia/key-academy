

  create or replace view `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
  OPTIONS()
  as -- models/staging/ga4/base_ga4_events.sql


SELECT
  -- Event metadata
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_name,
  user_pseudo_id,
  
  -- Device info
  device.category AS device_category,
  device.web_info.hostname AS web_hostname,
  device.operating_system AS operating_system,
  
  -- Location info
  geo.country AS country,
  geo.region AS region,
  geo.city AS city,
  
  -- Unnested parameters (flattened)
  ep.key AS param_key,
  ep.value.string_value AS param_string_value,
  ep.value.int_value AS param_int_value,
  ep.value.float_value AS param_float_value,
  ep.value.double_value AS param_double_value,
  
  -- Raw event data (for reference)
  event_params,
  device,
  geo

FROM `intrepid-craft-450709-f9`.`ga4_copy`.`events_*`,
UNNEST(event_params) AS ep;

