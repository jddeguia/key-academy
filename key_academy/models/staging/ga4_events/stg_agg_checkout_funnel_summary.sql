{{ config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['platform']
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('base_ga4_events') }}
    WHERE event_name IN ('first_visit', 'session_start', 'begin_checkout', 'add_payment_info', 'purchase')
),

summary AS (
    SELECT
        event_date,
        platform,
        is_active_user,
        SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
        SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
        SUM(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END) AS payment_details,
        SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS complete
    FROM base
    GROUP BY ALL
)

SELECT * FROM summary