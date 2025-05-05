
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_campaign_cross_channel`
      
    partition by event_date
    cluster by campaign_id, event_name, source_platform, user_id

    OPTIONS()
    as (
      

SELECT
    event_timestamp,
    event_date,
    COALESCE(user_id, user_pseudo_id) AS user_id,
    user_pseudo_id,
    event_name,
    
    -- Cross Channel Campaign Fields
    cross_channel_campaign_id AS campaign_id,
    cross_channel_campaign_name AS campaign_name,
    cross_channel_source AS source,
    cross_channel_medium AS medium,
    cross_channel_source_platform AS source_platform,
    cross_channel_default_group AS default_channel_group,
    cross_channel_primary_group AS primary_channel_group,

    -- Additional dimensions
    stream_id,
    platform,
    country,
    device_category,

    -- Metrics
    purchase_revenue_in_usd

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE cross_channel_campaign_id IS NOT NULL
    );
  