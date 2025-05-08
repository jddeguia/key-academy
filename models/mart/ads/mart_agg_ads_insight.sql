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

post_clicks AS (
  SELECT
    event_date,
    {{ standardize_platform(
        'collected_traffic_source_manual_source',
        'manual_source',
        'traffic_source_source'
    ) }} AS platform,
    SUM(CASE WHEN event_name IN ('session_start') THEN 1 ELSE 0 END) AS sessions,
    SUM(CASE WHEN event_name IN ('sign_up_request', 'sign_up', 'start_trial') THEN 1 ELSE 0 END) AS registrations,
    SUM(CASE WHEN event_name IN ('begin_checkout') THEN 1 ELSE 0 END) AS begin_checkout,
    SUM(CASE WHEN event_name IN ('purchase') THEN 1 ELSE 0 END) AS purchases,
  FROM {{ ref('base_ga4_events') }}
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
    LEFT JOIN post_clicks post USING (event_date, platform)
)

SELECT * FROM summary