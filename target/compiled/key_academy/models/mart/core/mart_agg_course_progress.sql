

WITH module_progress AS (
    SELECT *
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_user_module_progress`
),

kpis AS (
    SELECT
        DATE(module_started_at) AS module_started_date,
        course_title,
        CONCAT('Q', EXTRACT(QUARTER FROM DATE(module_started_at)), ' ', EXTRACT(YEAR FROM DATE(module_started_at))) AS cohort,
        SUM(CASE WHEN module_started_at IS NOT NULL AND module_unlocked_at IS NULL THEN 1 ELSE 0 END) AS trials,
        SUM(CASE WHEN module_unlocked_at IS NOT NULL THEN 1 ELSE 0 END) AS trials_ended,
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