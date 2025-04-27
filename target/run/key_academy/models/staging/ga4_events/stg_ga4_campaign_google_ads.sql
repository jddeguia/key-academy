
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_campaign_google_ads`
      
    partition by event_date
    cluster by google_ads_campaign_name, google_ads_account_name

    OPTIONS()
    as (
      

SELECT
  -- Event identifiers
    event_timestamp,
    event_date,
    user_pseudo_id,
    COALESCE(user_id, user_pseudo_id) AS user_id,
    event_name,

    -- Google Ads campaign fields (from session_traffic_source_last_click)
    google_ads_customer_id,
    google_ads_account_name,
    google_ads_campaign_id,
    google_ads_campaign_name,
    google_ads_ad_group_id,
    google_ads_ad_group_name,

    -- Click identifiers
    collected_traffic_source_gclid AS gclid,

    -- Traffic source (for comparison)
    traffic_source_source,
    traffic_source_medium,

    -- Device/geo context
    device_category,
    country,
    region,

    -- Conversion metrics
    purchase_revenue_in_usd,
    transaction_id

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE 
  -- Only include Google Ads traffic
  (
    google_ads_campaign_id IS NOT NULL
    OR collected_traffic_source_gclid IS NOT NULL
  )
    );
  