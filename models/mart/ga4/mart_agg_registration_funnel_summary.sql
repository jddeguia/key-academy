{{ config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['platform']
) }}

SELECT * 
FROM {{ ref('stg_agg_registration_funnel_summary') }}
    