{{
    config(
      materialized='table'
    )
}}

SELECT *
FROM {{ source('raw_data', 'test_orders_raw__stream_billing_orders') }}
