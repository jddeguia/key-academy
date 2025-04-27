{{
    config(
      materialized='table'
    )
}}

SELECT * FROM {{ ref('stg_billing_products')}}   