{{
    config(
      materialized='table'
    )
}}

WITH modules AS (
    SELECT 
        node_id AS module_id,
        title AS module_title
    FROM {{ ref('mart_trees_published_nodes') }}
    WHERE structure_type != 'Root'
),

courses AS (
    SELECT 
        node_id AS root_node_id,
        title AS course_title
    FROM {{ ref('mart_trees_published_nodes') }}
    WHERE structure_type = 'Root'
),

learn_tree_states AS (
    SELECT
        user_id,
        root_node_id,
        head_content_node_id AS module_id,
        started_at AS module_started_at,
        status AS module_progress_status
    FROM {{ ref('mart_learn_tree_states') }}
),

module_unlocks AS (
    SELECT
        user_id,
        root_node_id,
        tree_unlocked_state_datetimestamp AS module_unlocked_at,
        learn_tree_unlocked_kind,
        license_id
    FROM {{ ref('mart_learn_tree_unlocked_states') }}
),

certificates AS (
    SELECT
        recipient_id AS user_id,
        ordered_at AS certificate_completed_at,
        certificate_id,
        verification_number,
        received_in_root_node_id AS root_node_id,
        received_in_node_id AS module_id,
        is_with_honors,
        performance_in_percent
    FROM {{ ref('mart_learn_ihk_certificate_orders') }}
    WHERE certificate_kind = 'Ordered'
)

SELECT
    ts.user_id,
    ts.root_node_id,
    c.course_title,
    ts.module_id,
    m.module_title,
    ts.module_started_at,
    ts.module_progress_status,
    mu.module_unlocked_at,
    mu.learn_tree_unlocked_kind,
    cr.certificate_completed_at,
    cr.certificate_id,
    cr.verification_number,
    cr.is_with_honors,
    cr.performance_in_percent,
    mu.license_id
FROM learn_tree_states ts
LEFT JOIN module_unlocks mu USING (user_id, root_node_id)
LEFT JOIN modules m ON ts.module_id = m.module_id
LEFT JOIN courses c ON ts.root_node_id = c.root_node_id
LEFT JOIN certificates cr ON cr.user_id = ts.user_id 
    AND cr.root_node_id = ts.root_node_id 
    AND cr.module_id = ts.module_id
