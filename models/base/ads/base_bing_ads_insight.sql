{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        TimePeriod AS date,
        CampaignName AS campaign_name,
        Spend AS spend,
        Impressions AS impressions,
        Clicks AS clicks,
        DeviceType AS platform,
        Conversions AS conversions
    FROM {{ source('bing_ads', 'campaign_performance_report_daily') }}
)

SELECT * FROM source