
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_campaign_manual`
      
    partition by event_date
    cluster by event_name, user_id

    OPTIONS()
    as (
      

SELECT
  -- Event info
  event_timestamp,
  event_date,
  event_name,  -- Keep for analysis
  
  -- User info
  user_pseudo_id,
  COALESCE(user_id, user_pseudo_id) AS user_id,
  
  -- Core UTM parameters (renamed for clarity)
  collected_traffic_source_manual_campaign_id AS campaign_id,
  collected_traffic_source_manual_campaign_name AS campaign_name,
  collected_traffic_source_manual_source AS utm_source,
  collected_traffic_source_manual_medium AS utm_medium,
  collected_traffic_source_manual_term AS utm_term,
  collected_traffic_source_manual_content AS utm_content,
  
  -- Additional campaign context
  collected_traffic_source_manual_source_platform AS source_platform,
  collected_traffic_source_gclid,
  collected_traffic_source_dclid,
  
  -- Traffic source (for comparison)
  traffic_source_source,
  traffic_source_medium,
  
  -- Device/geo context
  device_category,
  country,
  region

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE 
  -- Only include rows where ANY campaign parameter exists
  (
    collected_traffic_source_manual_campaign_id IS NOT NULL
    OR collected_traffic_source_manual_campaign_name IS NOT NULL
    OR collected_traffic_source_manual_source IS NOT NULL
    OR collected_traffic_source_manual_medium IS NOT NULL
  )
    );
  