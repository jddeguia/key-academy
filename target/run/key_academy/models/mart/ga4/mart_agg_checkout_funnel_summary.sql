
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`mart_agg_checkout_funnel_summary`
      
    partition by created_date
    cluster by customer_type

    OPTIONS()
    as (
      

SELECT * 
FROM `intrepid-craft-450709-f9`.`key_academy`.`stg_agg_checkout_funnel_summary`
    );
  