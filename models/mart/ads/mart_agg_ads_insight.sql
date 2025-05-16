{{
  config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['platform']
  )
}}

WITH pre_clicks AS (
    SELECT
        date AS event_date,
        platform,
        SUM(spend) AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions
    FROM {{ ref('mart_ads_insight') }}
    GROUP BY ALL
),

post_clicks_raw AS (
  SELECT
    event_date,
    user_pseudo_id,
    event_timestamp,
    {{ standardize_platform(
        'collected_traffic_source_manual_source',
        'manual_source',
        'traffic_source_source'
    ) }} AS platform,
    event_name
  FROM {{ ref('base_ga4_events') }}
  WHERE event_name IN (
    'session_start', 'sign_up_request', 'sign_up', 'start_trial', 
    'begin_checkout', 'purchase'
  )
),

post_clicks_deduped AS (
  SELECT
    event_date,
    platform,
    COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_pseudo_id END) AS sessions,
    COUNT(DISTINCT CASE WHEN event_name IN ('sign_up_request', 'sign_up', 'start_trial') THEN user_pseudo_id END) AS registrations,
    COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_pseudo_id END) AS begin_checkout,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchases
  FROM post_clicks_raw
  GROUP BY ALL
),

summary AS (
    SELECT
        pre.event_date,
        pre.platform,
        post.sessions,
        post.registrations,
        post.begin_checkout,
        post.purchases,
        pre.spend,
        pre.impressions,
        pre.clicks,
        pre.conversions
    FROM pre_clicks pre
    LEFT JOIN post_clicks_deduped post USING (event_date, platform)
)

SELECT * FROM summary
