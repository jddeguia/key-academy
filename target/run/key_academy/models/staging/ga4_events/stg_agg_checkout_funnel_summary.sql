
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_agg_checkout_funnel_summary`
      
    partition by event_date
    cluster by platform

    OPTIONS()
    as (
      

SELECT
  event_date,
  platform,
  SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
  SUM(CASE WHEN event_name = 'sign_up' THEN 1 ELSE 0 END) AS create_account,
  SUM(CASE WHEN event_name = 'sign_up_request' THEN 1 ELSE 0 END) AS create_password,
  SUM(CASE WHEN event_name = 'login' THEN 1 ELSE 0 END) AS confirm_email
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
GROUP BY event_date, platform
    );
  