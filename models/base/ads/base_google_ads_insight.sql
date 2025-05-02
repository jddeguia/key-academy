{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        segments_date AS date,
        campaign_base_campaign AS campaign_name,
        metrics_cost_micros / 1000000 AS spend,
        metrics_impressions AS impressions,
        metrics_clicks AS clicks,
        segments_device AS platform,
        metrics_conversions_value AS conversions
    FROM {{ source('google_ads', 'p_ads_CampaignStats_8244972127') }}
)

SELECT * FROM source