{{
    config(
      materialized='table'
    )
}}

WITH module_progress AS (
    SELECT *
    FROM {{ ref('mart_user_module_progress') }}
),

course_summary AS (
    SELECT
        user_id,
        root_node_id,
        MIN(module_started_at) AS course_start_at,
        COUNT(*) AS total_modules,
        COUNTIF(module_progress_status = 'Started') AS modules_started,
        COUNTIF(module_progress_status = 'Finished') AS modules_finished,
        MAX(module_unlocked_at) AS latest_unlock_time,
        MAX(learn_tree_unlocked_kind) AS unlock_kind,
        MAX(license_id) AS license_id
    FROM module_progress
    GROUP BY user_id, root_node_id
),

course_titles AS (
    SELECT
        node_id AS root_node_id,
        title AS course_title
    FROM {{ ref('mart_trees_published_nodes') }}
    WHERE structure_type = 'Root'
)

SELECT
    cs.user_id,
    cs.root_node_id,
    ct.course_title,
    cs.course_start_at,
    cs.total_modules,
    cs.modules_started,
    cs.modules_finished,
    cs.unlock_kind,
    cs.license_id
FROM course_summary cs
LEFT JOIN course_titles ct USING (root_node_id)
