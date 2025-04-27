
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_account_events`
      
    partition by timestamp_trunc(event_timestamp, day)
    cluster by event_name, user_id

    OPTIONS()
    as (
      

SELECT *
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN ('sign_up_request', 'login', 'sign_up', 'start_trial', 'end_trial')
    );
  