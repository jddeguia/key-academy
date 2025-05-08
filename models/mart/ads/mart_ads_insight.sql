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
        source AS platform,
        CASE
          WHEN LOWER(platform) IN ('mobile', 'mobile_web', 'mobile_app', 'smartphone') THEN 'MOBILE'
          WHEN LOWER(platform) IN ('desktop', 'computer') THEN 'DESKTOP'
          WHEN LOWER(platform) IN ('tablet', 'tablet') THEN 'TABLET' 
          WHEN LOWER(platform) = 'connected_tv' THEN 'TV'
          WHEN LOWER(platform) IN ('unknown', 'other') THEN 'OTHERS' 
          ELSE UPPER(platform)
        END AS device_type,
        spend,
        impressions,
        clicks,
        conversions
    FROM staging
)

SELECT * FROM summary