
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_events_unflattened`
      
    
    

    OPTIONS()
    as (
      

WITH base_data AS (
    SELECT * EXCEPT (event_date),
    CAST(event_date AS DATE) as event_date
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events_unflattened`
)

SELECT * FROM base_data
    );
  