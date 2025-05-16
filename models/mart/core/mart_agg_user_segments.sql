{{
    config(
      materialized='table'
    )
}}

WITH user_info AS (
    SELECT
        user_id,
        COALESCE(CONCAT(business_kind || ' - ' || age_bucket), 'Unknown') AS segments
    FROM {{ ref('mart_user_segments') }}
),

user_module_progress AS (
    SELECT
        user_id,
        DATE(module_started_at) AS module_started_date,
        SUM(CASE WHEN module_started_at IS NOT NULL AND module_unlocked_at IS NULL THEN 1 ELSE 0 END) AS trials,
        COUNTIF(module_progress_status = 'Started') AS courses_started,
        COUNTIF(module_progress_status = 'Finished') AS lessons_completed,
        SUM(CASE WHEN certificate_id IS NOT NULL THEN 1 ELSE 0 END) AS certificates,
        SUM(CASE WHEN license_id IS NOT NULL THEN 1 ELSE 0 END) AS licenses,
        SUM(net_price) AS revenue
    FROM {{ ref('mart_user_module_progress') }}
    GROUP BY ALL
),

summary AS (
    SELECT
        mod.user_id,
        user.segments,
        mod.module_started_date,
        mod.trials,
        mod.courses_started,
        mod.lessons_completed,
        mod.certificates,
        mod.licenses,
        mod.revenue
    FROM user_module_progress mod
    FULL JOIN user_info user USING (user_id)
)

SELECT * FROM summary