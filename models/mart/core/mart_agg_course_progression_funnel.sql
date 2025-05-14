{{
    config(
      materialized='table'
    )
}}

WITH course_stats AS (
    SELECT
        COALESCE(course_title, 'Unknown Course') AS course_title,
        user_id,
        -- Simple counts without date logic
        MAX(CASE WHEN license_id IS NULL THEN 1 ELSE 0 END) AS started_trial,
        MAX(CASE WHEN license_id IS NULL AND module_progress_status = 'Completed' THEN 1 ELSE 0 END) AS completed_trial_content,
        MAX(CASE WHEN license_id IS NOT NULL THEN 1 ELSE 0 END) AS obtained_license,
        MAX(CASE WHEN certificate_completed_at IS NOT NULL THEN 1 ELSE 0 END) AS earned_certificate,
        SUM(CASE WHEN module_progress_status = 'Completed' THEN 1 ELSE 0 END) AS modules_completed
    FROM {{ ref('mart_user_module_progress') }}
    GROUP BY course_title, user_id
)

SELECT
    course_title,
    SUM(started_trial) AS users_started_trial,
    SUM(obtained_license) AS users_obtained_license,
    SUM(earned_certificate) AS users_earned_certificate,
FROM course_stats
GROUP BY course_title
