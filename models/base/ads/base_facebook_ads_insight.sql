{{ config(materialized = 'view') }}

WITH source AS (
    SELECT
        created_time AS date,
        campaign_name,
        spend,
        impressions,
        clicks,
        device_platform AS platform,
        unique_clicks AS conversions
    FROM {{ source('test_orders', 'ads_insights_delivery_platform_and_device_platform') }}
)

SELECT * FROM source