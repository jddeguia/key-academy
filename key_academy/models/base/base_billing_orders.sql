{{
    config(
      materialized='table'
    )
}}

WITH biling_orders_base AS (
  SELECT 
    _airbyte_raw_id AS airbyte_raw_id,
    _airbyte_extracted_at AS airbyte_extracted_at,
    _airbyte_generation_id AS airbyte_generation_id,
    REPLACE(id, 'Order:', '') AS order_id,
    _id AS id,
    status AS order_status,
    createdAt AS created_at,
    _ab_cdc_cursor AS ab_cdc_cursor,
    _ab_cdc_updated_at AS ab_cdc_updated_at
  FROM {{ source('raw_test_billing_orders', 'billing_orders')}}
),

unnest_columns AS (
  SELECT * 
  FROM biling_orders_base
)

SELECT * FROM unnest_columns
