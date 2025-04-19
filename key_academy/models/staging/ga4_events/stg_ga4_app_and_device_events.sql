{{ config(
    materialized='table',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['event_name', 'user_id', 'device_category']
) }}

SELECT *
FROM {{ ref('base_ga4_events') }}
WHERE event_name IN ('app_remove', 'app_update', 'os_update')
