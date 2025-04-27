
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_app_and_device_events`
      
    partition by timestamp_trunc(event_timestamp, day)
    cluster by event_name, user_id, device_category

    OPTIONS()
    as (
      

SELECT *
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN ('app_remove', 'app_update', 'os_update')
    );
  