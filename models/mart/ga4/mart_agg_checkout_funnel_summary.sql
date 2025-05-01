{{ config(
    materialized='table',
    partition_by={'field': 'created_date', 'data_type': 'date'},
    cluster_by=['customer_type']
) }}

SELECT * 
FROM {{ ref('stg_agg_checkout_funnel_summary') }}
    