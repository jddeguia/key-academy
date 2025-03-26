{{
    config(
      materialized='table'
    )
}}

SELECT *
FROM {{ source('raw_test_billing_orders', 'billing_orders') }}
