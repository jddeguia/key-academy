{{
  config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['platform']
  )
}}

SELECT
  event_date,
  platform,
  -- Simple counts of each event type
  SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
  SUM(CASE WHEN event_name = 'sign_up' THEN 1 ELSE 0 END) AS create_account,
  SUM(CASE WHEN event_name = 'sign_up_request' THEN 1 ELSE 0 END) AS create_password,
  SUM(CASE WHEN event_name = 'login' THEN 1 ELSE 0 END) AS confirm_email
FROM {{ ref('base_ga4_events') }}
WHERE event_name IN ('session_start', 'sign_up', 'sign_up_request', 'login')
GROUP BY event_date, platform