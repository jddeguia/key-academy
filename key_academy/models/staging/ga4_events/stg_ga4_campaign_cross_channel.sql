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
      'event_name',
      'source_platform',
      'user_id'
    ]
  )
}}

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

FROM {{ ref('base_ga4_events') }}
WHERE cross_channel_campaign_id IS NOT NULL