{{ config(
    materialized='table',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['event_name', 'user_id']
) }}

SELECT *
FROM {{ ref('base_ga4_events') }}
WHERE event_name IN ('firebase_campaign', 'file_download', 'ga_download_brochure')
