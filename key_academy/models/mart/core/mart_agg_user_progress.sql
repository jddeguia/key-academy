{{
    config(
      materialized='table'
    )
}}

SELECT
    user_id,
    COUNT(DISTINCT root_node_id) AS total_courses_enrolled,
    SUM(total_modules) AS total_modules_available,
    SUM(modules_started) AS total_modules_started,
    SUM(modules_finished) AS total_modules_finished,
    SAFE_DIVIDE(SUM(modules_finished), SUM(total_modules)) AS overall_completion_rate,
    COUNTIF(certificate_id IS NOT NULL) AS total_certificates_earned,
    COUNTIF(LOWER(is_with_honors) = 'true') AS total_certificates_with_honors,
    SAFE_DIVIDE(
        COUNTIF(LOWER(is_with_honors) = 'true'),
        COUNTIF(certificate_id IS NOT NULL)
    ) AS honors_rate
FROM {{ ref('mart_user_course_progress') }}
GROUP BY user_id
