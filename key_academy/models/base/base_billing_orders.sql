{{
    config(
      materialized='table'
    )
}}

WITH billing_orders_base AS (
  SELECT 
    _airbyte_raw_id AS airbyte_raw_id,
    _airbyte_extracted_at AS airbyte_extracted_at,
    JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
    _airbyte_generation_id AS airbyte_generation_id,
    REPLACE(id, 'Order:', '') AS order_id,  
    _id AS id,
    JSON_QUERY(acl, '$') AS acl,
    JSON_QUERY(cart, '$') AS cart,
    status AS billing_order_status,
    createdAt AS created_at,
    JSON_QUERY(purchaser, '$') AS purchaser,
    JSON_QUERY(invoiceData, '$') AS invoice_data,
    JSON_QUERY(paymentData, '$') AS payment_data,
    JSON_QUERY(deletionInfo, '$') AS deletion_info,
    _ab_cdc_cursor AS ab_cdc_cursor,
    JSON_QUERY(customerDetails, '$') AS customer_details,
    _ab_cdc_deleted_at AS ab_cdc_deleted_at,
    _ab_cdc_updated_at AS ab_cdc_updated_at,
    JSON_QUERY(allowedPaymentMethods, '$') AS allowed_payment_methods,
    JSON_QUERY(selectedPaymentMethod, '$') AS selected_payment_method
  FROM {{ source('test_orders', 'billing_orders')}}
),

unnest_data AS (
  SELECT
    airbyte_extracted_at,
    airbyte_generation_id,
    ab_cdc_deleted_at,
    ab_cdc_updated_at,
    airbyte_raw_id,
    ab_cdc_cursor,
    order_id,  
    id,
    billing_order_status,
    created_at,
    ARRAY_TO_STRING(JSON_VALUE_ARRAY(allowed_payment_methods, '$.paymentMethods'), ', ') AS allowed_payment_methods
  FROM billing_orders_base
  
)

SELECT * FROM unnest_data


