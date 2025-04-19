{{
    config(
      materialized='table'
    )
}}

WITH base_billing_products AS (
    SELECT
        id,
        product_id,
        title AS product_title,
        is_hidden,
        net_price,
        is_tax_free,
        license_kind,
        is_deleted
    FROM {{ ref('base_billing_products')}}   
)

SELECT * FROM base_billing_products