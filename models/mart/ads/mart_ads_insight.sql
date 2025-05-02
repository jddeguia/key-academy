{{
  config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['platform']
  )
}}

WITH staging AS (
    SELECT * 
    FROM {{ ref('stg_ads_insight') }}
),

summary AS (
    SELECT
        date,
        campaign_name,
        source,
        platform,
        SUM(spend) AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions
    FROM staging
    GROUP BY ALL
)

SELECT * FROM summary