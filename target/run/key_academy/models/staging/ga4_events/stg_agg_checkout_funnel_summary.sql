
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_agg_checkout_funnel_summary`
      
    partition by created_date
    cluster by customer_type

    OPTIONS()
    as (
      

WITH user_first_appearance AS (
  SELECT
    user_id,
    MIN(created_at) AS first_appearance_date
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_orders`
  GROUP BY user_id
),

base_with_customer_type AS (
  SELECT
    order_id,
    created_at,
    DATE(created_at) AS created_date,
    user_id,
    billing_order_status,
    CASE
      WHEN created_at = ufa.first_appearance_date THEN 'New'
      ELSE 'Returning'
    END AS customer_type
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_orders`
  LEFT JOIN user_first_appearance ufa USING(user_id)
)

SELECT
  created_date,
  customer_type,
  SUM(CASE WHEN billing_order_status = 'HasCart' THEN 1 ELSE 0 END) AS has_cart,
  SUM(CASE WHEN billing_order_status = 'HasBillingDetails' THEN 1 ELSE 0 END) AS has_billing_details,
  SUM(CASE WHEN billing_order_status = 'HasPaymentDetails' THEN 1 ELSE 0 END) AS has_payment_details,
  SUM(CASE WHEN billing_order_status = 'Purchased' THEN 1 ELSE 0 END) AS purchased,
FROM base_with_customer_type
GROUP BY ALL
    );
  