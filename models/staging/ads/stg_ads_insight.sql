{{
  config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['platform']
  )
}}

WITH bing_ads AS (
    SELECT *,
        "BING" AS source
    FROM {{ ref('base_bing_ads_insight') }}
),

facebook_ads AS (
    SELECT *,
        "FACEBOOK" AS source
    FROM {{ ref('base_facebook_ads_insight') }}
),

google_ads AS (
    SELECT *,
        "GOOGLE" AS source
    FROM {{ ref('base_google_ads_insight') }}
),

summary AS (
    SELECT
        date,
        campaign_name,
        spend,
        impressions,
        clicks,
        platform,
        conversions,
        source
    FROM bing_ads

    UNION ALL

    SELECT
        date,
        campaign_name,
        spend,
        impressions,
        clicks,
        platform,
        conversions,
        source
    FROM facebook_ads

    UNION ALL

    SELECT
        date,
        campaign_name,
        spend,
        impressions,
        clicks,
        platform,
        conversions,
        source
    FROM google_ads
)

SELECT * FROM summary


