
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_cart_and_checkout_events`
      
    partition by timestamp_trunc(event_timestamp, day)
    cluster by event_name, user_id

    OPTIONS()
    as (
      

SELECT *
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN ('view_cart', 'website_add_to_cart', 'add_to_cart', 'remove_from_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info')
    );
  