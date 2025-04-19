{{ config(
    materialized='table',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['event_name', 'user_id']
) }}

SELECT *
FROM {{ ref('base_ga4_events') }}
WHERE event_name IN ('view_cart', 'website_add_to_cart', 'add_to_cart', 'remove_from_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info')
