

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
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_products`   
)

SELECT * FROM base_billing_products