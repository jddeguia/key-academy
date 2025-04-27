

SELECT
  -- Event metadata
    event_timestamp,
    event_date,
    
    -- User identifiers
    user_pseudo_id,
    COALESCE(user_id, user_pseudo_id) AS user_id,
    
    -- Lifetime Value metrics (direct from GA4)
    revenue AS ltv_revenue,
    currency AS ltv_currency,
    
    -- User acquisition info
    user_first_touch_timestamp,
    traffic_source_source AS acquisition_source,
    traffic_source_medium AS acquisition_medium,
    traffic_source_name AS acquisition_campaign,
    
    -- Platform context
    platform,
    device_category,
    device_operating_system,
    
    -- Event context
    event_name,
    event_value_in_usd,
    
    -- Raw fields for downstream flexibility
    user_properties_key,
    user_properties_string_value,
    user_properties_int_value,
    user_properties_float_value,
    user_properties_double_value,

    event_param_key,
    event_param_string_value,
    event_param_int_value,
    event_param_float_value,
    event_param_double_value,

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE
  -- Filter for events with LTV data or monetization events
  (
    revenue IS NOT NULL
    OR event_name IN (
      'purchase',
      'in_app_purchase',
      'start_trial',
      'end_trial',
      'subscribe',
      'add_payment_info',
      'begin_checkout',
      'ecommerce_purchase'
    )
  )