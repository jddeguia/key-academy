
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_agg_registration_funnel_summary`
      
    partition by event_date
    cluster by platform

    OPTIONS()
    as (
      

SELECT
  event_date,
  platform,
  -- Simple counts of each event type
  SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
  SUM(CASE WHEN event_name = 'sign_up_request' THEN 1 ELSE 0 END) AS sign_up_request,
  SUM(CASE WHEN event_name = 'sign_up' THEN 1 ELSE 0 END) AS sign_up,
  SUM(CASE WHEN event_name = 'start_trial' THEN 1 ELSE 0 END) AS start_trial
FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE event_name IN ('session_start', 'sign_up', 'sign_up_request', 'start_trial')
GROUP BY event_date, platform
    );
  