{{
  config(
    materialized = 'table',
  )
}}

WITH user_info AS (
    SELECT 
        user_id,
        company_type,
        business_kind,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) < 10 THEN '0-9'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 10 AND 19 THEN '10-19'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 20 AND 29 THEN '20-29'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 30 AND 39 THEN '30-39'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 40 AND 49 THEN '40-49'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 50 AND 59 THEN '50-59'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 60 AND 69 THEN '60-69'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 70 AND 79 THEN '70-79'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 80 AND 89 THEN '80-89'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(customer_date_of_birth), YEAR) BETWEEN 90 AND 99 THEN '90-99'
            ELSE '100+'
        END AS age_bucket
    FROM {{ ref('base_billing_orders') }}
)

SELECT * FROM user_info