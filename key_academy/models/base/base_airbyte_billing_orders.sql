{{
    config(
      materialized='table'
    )
}}

WITH airbyte_base_data AS (
  SELECT 
    _airbyte_raw_id AS airbyte_raw_id,
    _airbyte_extracted_at AS airbyte_extracted_at,
    _airbyte_loaded_at AS airbyte_loaded_at,
    _airbyte_generation_id AS airbyte_generation_id,
    JSON_QUERY(_airbyte_data, '$')  AS airbyte_data, 
    JSON_QUERY(_airbyte_meta, '$')  AS airbyte_meta,
  FROM {{ source('raw_airbyte_billing_orders', 'test_orders_raw__stream_billing_orders') }}
),

unnest_data AS (
  SELECT 
    airbyte_raw_id,
    airbyte_extracted_at,
    airbyte_loaded_at,
    airbyte_generation_id,
    JSON_VALUE(airbyte_data, '$._id') AS id,
    JSON_VALUE(airbyte_data, '$.id') AS order_id,
    JSON_VALUE(airbyte_data, '$.status') AS status,
    JSON_VALUE(airbyte_data, '$.createdAt') AS created_at,
    JSON_VALUE(payment_method, '$.value') AS payment_method,
    JSON_VALUE(risk_assessment, '$.value') AS risk_assessment,
    JSON_VALUE(airbyte_data, '$.invoiceData.kind') AS invoice_kind,
    JSON_VALUE(airbyte_data, '$.deletionInfo.isDeleted') AS is_deleted,
    JSON_VALUE(airbyte_data, '$.deletionInfo.kind') AS deletion_kind,
    JSON_VALUE(airbyte_data, '$.purchaser.accountId') AS purchaser_account_id
  FROM airbyte_base_data
  LEFT JOIN UNNEST(JSON_VALUE_ARRAY(airbyte_data, '$.allowedPaymentMethods.paymentMethods')) AS payment_method
  LEFT JOIN UNNEST(JSON_VALUE_ARRAY(airbyte_data, '$.allowedPaymentMethods.additionalInformation.RiskAssessments')) AS risk_assessment
)

SELECT * FROM unnest_data