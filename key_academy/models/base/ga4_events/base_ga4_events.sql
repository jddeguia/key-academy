{{ config(materialized = 'view') }}

SELECT
  -- Event metadata
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
  TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  
  event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,

  -- Privacy info
  privacy_info.analytics_storage,
  privacy_info.ads_storage,
  privacy_info.uses_transient_token,

  -- User properties
  up.key AS user_properties_key,
  up.value.string_value AS user_properties_string_value,
  up.value.int_value AS user_properties_int_value,
  up.value.float_value AS user_properties_float_value,
  up.value.double_value AS user_properties_double_value,

  -- User LTV
  user_ltv.revenue,
  user_ltv.currency,
  
  -- User info
  event_name,
  user_id,
  user_pseudo_id,
  
  -- Device info
  device.category AS device_category,
  device.mobile_brand_name AS device_mobile_brand_name,
  device.mobile_model_name AS device_mobile_model_name,
  device.mobile_marketing_name AS device_mobile_marketing_name,
  device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
  device.operating_system AS device_operating_system,
  device.operating_system_version AS device_operating_system_version,
  device.vendor_id AS device_vendor_id,
  device.advertising_id AS device_advertising_id,
  device.language AS device_language,
  device.is_limited_ad_tracking AS device_is_limited_ad_tracking,
  device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
  device.browser AS device_browser,
  device.browser_version AS device_browser_version,

  
  -- Location info
  geo.country AS country,
  geo.region AS region,
  geo.city AS city,
  
  -- Event parameters
  ep.key AS event_param_key,
  ep.value.string_value AS event_param_string_value,
  ep.value.int_value AS event_param_int_value,
  ep.value.float_value AS event_param_float_value,
  ep.value.double_value AS event_param_double_value,


FROM {{ source('google_analytics', 'events_*') }},
UNNEST(event_params) AS ep,
UNNEST(user_properties) AS up