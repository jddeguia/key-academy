
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`mart_agg_course_progress`
      
    
    

    OPTIONS()
    as (
      

WITH module_progress AS (
    SELECT *
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_user_module_progress`
),

kpis AS (
    SELECT
        DATE(module_started_at) AS module_started_date,
        course_title,
        SUM(CASE WHEN module_started_at IS NOT NULL AND module_unlocked_at IS NULL THEN 1 ELSE 0 END) AS trials,
        COUNTIF(module_progress_status = 'Started') AS courses_started,
        COUNTIF(module_progress_status = 'Finished') AS lessons_completed,
        SUM(CASE WHEN certificate_id IS NOT NULL THEN 1 ELSE 0 END) AS certificates,
        SUM(CASE WHEN license_id IS NOT NULL THEN 1 ELSE 0 END) AS licenses,
        SUM(net_price) AS revenue
    FROM module_progress
    GROUP BY ALL
)

SELECT * FROM kpis
ORDER BY module_started_date
    );
  