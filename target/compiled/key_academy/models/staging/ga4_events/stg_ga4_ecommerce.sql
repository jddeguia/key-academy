

SELECT
  -- Event metadata
    event_timestamp,
    event_date,
    event_name,

    -- User identifiers
    user_pseudo_id,
    COALESCE(user_id, user_pseudo_id) AS user_id,

    -- Ecommerce transaction info
    transaction_id AS ecommerce_transaction_id,
    purchase_revenue_in_usd AS purchase_revenue_usd,
    purchase_revenue AS purchase_revenue,
    refund_value_in_usd AS refund_amount_usd,
    refund_value AS refund_amount,
    shipping_value_in_usd AS shipping_value_usd,
    shipping_value AS shipping_value,
    tax_value_in_usd AS tax_value_usd,
    tax_value AS tax_value,
    total_item_quantity AS total_items,
    unique_items AS unique_items,

    -- item metadata
    item_id,
    item_name,
    item_brand,
    item_variant,
    item_category,
    item_category2,
    item_category3,
    item_category4,
    item_category5,
    price_in_usd,
    price,
    quantity,
    item_revenue_in_usd,
    item_revenue,
    item_refund_in_usd,
    item_refund,
    coupon,
    affiliation,
    location_id,
    item_list_id,
    item_list_name,
    item_list_index,
    promotion_id,
    promotion_name,
    creative_name,
    creative_slot,

    -- Raw fields for downstream processing
    user_properties_key,
    user_properties_string_value,
    user_properties_int_value,
    user_properties_float_value,
    user_properties_double_value,

    event_param_key,
    event_param_string_value,
    event_param_int_value,
    event_param_float_value,
    event_param_double_value,

    -- Traffic source
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source,

    -- Platform info
    platform,
    device_category,
    device_operating_system,
        

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN (
    'purchase',
    'refund',
    'add_payment_info',
    'add_shipping_info',
    'begin_checkout',
    'view_cart',
    'add_to_cart',
    'remove_from_cart',
    'view_item'
)