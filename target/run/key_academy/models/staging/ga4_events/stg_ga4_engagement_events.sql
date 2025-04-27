
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_engagement_events`
      
    partition by timestamp_trunc(event_timestamp, day)
    cluster by event_name, user_id

    OPTIONS()
    as (
      

SELECT *
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN ('session_start', 'page_view', 'first_visit', 'screen_view', 'user_engagement')
    );
  