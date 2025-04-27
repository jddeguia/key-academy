{{ config(
    materialized='table',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['event_name', 'user_id']
) }}

SELECT *
FROM {{ ref('base_ga4_events') }}
WHERE event_name IN ('sign_up_request', 'login', 'sign_up', 'start_trial', 'end_trial')
