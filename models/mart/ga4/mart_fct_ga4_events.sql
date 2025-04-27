{{ config(materialized = 'table') }}

WITH base_data AS (
    SELECT * 
    FROM {{ ref('stg_ga4_events_unflattened') }}
)

SELECT * FROM base_data

