{{ config(materialized = 'view') }}

WITH base_data AS (
    SELECT
        FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', CAST(event_date AS STRING))) AS event_date,
        event_name,
        platform AS location,
        event_params
    FROM {{ source('google_analytics', 'events_*') }}
)

SELECT * FROM base_data

