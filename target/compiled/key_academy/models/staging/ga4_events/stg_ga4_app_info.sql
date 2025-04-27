

SELECT
    event_timestamp,
    event_previous_timestamp,
    user_first_touch_timestamp,
    event_date,
    
    -- Event identifiers
    event_name,
    event_value_in_usd,
    event_bundle_sequence_id,
    
    -- User identifiers
    user_pseudo_id,
    COALESCE(user_id, user_pseudo_id) AS user_id,


    -- Platform info
    platform,
    stream_id,
    
    -- App info metadata
    app_info_id,
    app_info_version,
    app_install_source,
    app_firebase_app_id,
    app_install_store,
    
    -- Device info
    device_category,
    device_operating_system,
    device_operating_system_version,
    device_browser_version,
    
    -- Traffic source
    traffic_source_name,
    traffic_source_source,
    traffic_source_medium,
    
    -- Privacy info
    analytics_storage,
    ads_storage,
    uses_transient_token,
    
    batch_event_index,
    batch_page_id

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`