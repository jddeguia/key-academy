

WITH user_first_purchase AS (
  SELECT
    user_id,
    MIN(created_at) AS first_purchase_date
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_orders`
  WHERE billing_order_status = 'Purchased'
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
      WHEN billing_order_status = 'Purchased' AND created_at = ufp.first_purchase_date THEN 'New'
      WHEN billing_order_status = 'Purchased' AND created_at > ufp.first_purchase_date THEN 'Returning'
      ELSE NULL
    END AS customer_type
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_orders`
  LEFT JOIN user_first_purchase ufp USING(user_id)
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