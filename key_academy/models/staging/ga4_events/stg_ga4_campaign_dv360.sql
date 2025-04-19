{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by = [
      'campaign_id',
      'user_id',
      'country',
      'device_category'
    ]
  )
}}

SELECT
    event_timestamp,
    event_date,
    COALESCE(user_id, user_pseudo_id) AS user_id,
    user_pseudo_id,
    event_name,
    
    -- DV360 Campaign Fields
    dv360_campaign_id AS campaign_id,
    dv360_campaign_name AS campaign_name,
    dv360_source AS source,
    dv360_medium AS medium,
    dv360_advertiser_id AS advertiser_id,
    dv360_advertiser_name AS advertiser_name,
    dv360_creative_id AS creative_id,
    dv360_creative_format AS creative_format,
    dv360_creative_name AS creative_name,
    dv360_exchange_id AS exchange_id,
    dv360_exchange_name AS exchange_name,
    dv360_insertion_order_id AS insertion_order_id,
    dv360_insertion_order_name AS insertion_order_name,
    dv360_line_item_id AS line_item_id,
    dv360_line_item_name AS line_item_name,
    dv360_partner_id AS partner_id,
    dv360_partner_name AS partner_name,

    -- Additional dimensions
    stream_id,
    platform,
    country,
    device_category,

    -- Metrics
    purchase_revenue_in_usd,
    total_item_quantity

FROM {{ ref('base_ga4_events') }}
WHERE dv360_campaign_id IS NOT NULL