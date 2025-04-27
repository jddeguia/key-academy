
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`mart_fct_ga4_events`
      
    
    

    OPTIONS()
    as (
      

WITH base_data AS (
    SELECT * 
    FROM `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_events_unflattened`
)

SELECT * FROM base_data
    );
  