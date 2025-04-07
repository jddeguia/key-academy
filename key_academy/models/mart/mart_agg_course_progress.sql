{{
    config(
      materialized='table'
    )
}}

WITH course_details AS (
    SELECT *
    FROM {{ ref('mart_course_summary')}}   
),

course_stats AS (
    SELECT 
        user_id,
        root_node_id,
        course_title,
        COUNTIF(module_progress_status = 'Started' AND module_unlocked_datetimestamp IS NULL) AS num_courses_in_trial,
        COUNTIF(module_progress_status = 'Started' AND module_unlocked_datetimestamp IS NOT NULL) AS num_courses_in_progress,
        COUNTIF(learn_tree_unlocked_kind = 'UnlockedByAdmin') AS num_unlocked_by_admin,
        COUNTIF(learn_tree_unlocked_kind = 'UnlockedByLicense') AS num_unlocked_by_license,
        COUNTIF(module_progress_status = 'Finished') AS num_modules_finished
    FROM course_details
    GROUP BY ALL
)

SELECT * FROM course_stats