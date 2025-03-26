{{
    config(
      materialized='table'
    )
}}

SELECT *
FROM {{ source('raw_airbyte_billing_orders', 'test_orders_raw__stream_billing_orders') }}
