

WITH module_progress AS (
    SELECT *
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_user_module_progress`
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
    GROUP BY ALL
),

course_titles AS (
    SELECT
        node_id AS root_node_id,
        title AS course_title
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_trees_published_nodes`
    WHERE structure_type = 'Root'
),

certificates AS (
    SELECT
        recipient_id AS user_id,
        ordered_at AS certificate_completed_at,
        certificate_id,
        verification_number,
        received_in_root_node_id AS root_node_id,
        is_with_honors,
        performance_in_percent
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_learn_ihk_certificate_orders`
    WHERE certificate_kind = 'Ordered'
)

SELECT
    cs.user_id,
    cs.root_node_id,
    ct.course_title,
    cs.course_start_at,
    cr.certificate_completed_at,
    cr.certificate_id,
    cr.verification_number,
    cr.is_with_honors,
    cr.performance_in_percent,
    cs.total_modules,
    cs.modules_started,
    cs.modules_finished,
    cs.unlock_kind,
    cs.license_id
FROM course_summary cs
LEFT JOIN course_titles ct USING (root_node_id)
LEFT JOIN certificates cr USING (user_id, root_node_id)