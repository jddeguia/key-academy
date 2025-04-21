{{ config(materialized = 'table') }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_ga4_events_unflattened') }}
)

SELECT * FROM base_data

