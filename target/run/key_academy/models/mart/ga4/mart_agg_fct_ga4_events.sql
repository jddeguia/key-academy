
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`mart_agg_fct_ga4_events`
      
    partition by event_date
    cluster by event_name

    OPTIONS()
    as (
      



WITH base AS (
  SELECT 
    event_date,
    event_name,
    location,
    COUNT(*) as event_count
  FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_fct_ga4_events`
  GROUP BY ALL
)

SELECT 
  event_date,
  event_name,
  
  SUM(CASE WHEN location = 'WEB' THEN event_count ELSE 0 END) AS WEB,
  
  SUM(CASE WHEN location = 'ANDROID' THEN event_count ELSE 0 END) AS ANDROID,
  
  SUM(CASE WHEN location = 'IOS' THEN event_count ELSE 0 END) AS IOS
  
FROM base
GROUP BY ALL
    );
  