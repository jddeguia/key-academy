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
    created_at,
    billing_order_status,
    SAFE.JSON_VALUE(purchaser, '$.accountId') AS account_id,
    SAFE.JSON_VALUE(purchaser, '$.userId') AS user_id,
    SAFE.JSON_VALUE(customer_details, '$.city') AS customer_city,
    SAFE.JSON_VALUE(customer_details, '$.country') AS customer_country,
    SAFE.JSON_VALUE(customer_details, '$.firstName') AS customer_first_name,
    SAFE.JSON_VALUE(customer_details, '$.lastName') AS customer_last_name,
    SAFE.JSON_VALUE(customer_details, '$.houseNumber') AS customer_house_number,
    SAFE.JSON_VALUE(customer_details, '$.invoiceEmail') AS customer_invoice_email,
    SAFE.JSON_VALUE(customer_details, '$.phoneNumber') AS customer_phone_number,
    SAFE.JSON_VALUE(customer_details, '$.postalCode') AS customer_postal_code,
    SAFE.JSON_VALUE(customer_details, '$.salutation') AS customer_salutation,
    SAFE.JSON_VALUE(customer_details, '$.street') AS customer_street,
    SAFE.JSON_VALUE(customer_details, '$.data.company') AS customer_company,
    SAFE.JSON_VALUE(customer_details, '$.data.companyDetails') AS company_details,
    SAFE.JSON_VALUE(customer_details, '$.data.companyType') AS company_type,
    SAFE.JSON_VALUE(customer_details, '$.data.kind') AS business_kind,
    ARRAY_TO_STRING(JSON_VALUE_ARRAY(allowed_payment_methods, '$.paymentMethods'), ', ') AS allowed_payment_methods,
    SAFE.JSON_VALUE(selected_payment_method, '$.kind') AS selected_payment_kind,
    SAFE.JSON_VALUE(selected_payment_method, '$.paymentMethodType') AS selected_payment_method_type,
    SAFE.JSON_VALUE(selected_payment_method, '$.chosenOption.downPayment.grossPrice') AS down_payment_gross,
    SAFE.JSON_VALUE(selected_payment_method, '$.chosenOption.financedAmount.grossPrice') AS financed_amount_gross,
    SAFE.JSON_VALUE(selected_payment_method, '$.chosenOption.monthlyRate.grossPrice') AS monthly_rate_gross,
    SAFE.JSON_VALUE(selected_payment_method, '$.chosenOption.rateCount') AS rate_count,
    SAFE.JSON_VALUE(selected_payment_method, '$.chosenOption.totalAmount.grossPrice') AS total_amount_gross,
    SAFE.JSON_VALUE(payment_data, '$.iban') AS iban,
    SAFE.JSON_VALUE(payment_data, '$.kind') AS payment_kind,
    SAFE.JSON_VALUE(payment_data, '$.paymentMethod') AS payment_method,
    SAFE.JSON_VALUE(invoice_data, '$.createdAt') AS invoice_created_at,
    SAFE.JSON_VALUE(invoice_data, '$.invoiceId') AS invoice_id,
    SAFE.JSON_VALUE(invoice_data, '$.invoiceNumber') AS invoice_number,
    SAFE.JSON_VALUE(invoice_data, '$.kind') AS invoice_kind,
    SAFE.JSON_VALUE(invoice_data, '$.fileData.documentFileId') AS document_file_id,
    SAFE.JSON_VALUE(invoice_data, '$.fileData.fileDescriptionId') AS file_description_id,
    SAFE.JSON_VALUE(payment_data, '$.mandateInformation.acceptanceDateTime') AS mandate_acceptance_datetime,
    SAFE.JSON_VALUE(payment_data, '$.mandateInformation.mandateNumber') AS mandate_number,
    SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted,
    SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_kind
  FROM billing_orders_base
)

SELECT * FROM unnest_data


