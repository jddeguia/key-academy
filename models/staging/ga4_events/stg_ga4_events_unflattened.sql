{{ config(materialized = 'table') }}

WITH base_data AS (
    SELECT * EXCEPT (event_date),
    CAST(event_date AS DATE) as event_date
    FROM {{ ref('base_ga4_events_unflattened') }}
)

SELECT * FROM base_data