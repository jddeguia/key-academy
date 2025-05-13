{{
  config(
    materialized = 'table',
  )
}}

WITH user_info AS (
    SELECT 
        user_id,
        company_type,
        COALESCE(business_kind, 'Unknown') AS business_kind,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) < 29 THEN '<29'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 30 AND 39 THEN '30-39'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 40 AND 49 THEN '40-49'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) >= 50 THEN '50+'
            ELSE 'Unknown'
        END AS age_bucket
    FROM {{ ref('base_billing_orders') }}
)

SELECT * FROM user_info