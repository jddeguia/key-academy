{{
    config(
      materialized='table'
    )
}}

WITH learn_tree_published_nodes AS (
    SELECT 
        node_id AS root_node_id,
        title
    FROM {{ ref('mart_trees_published_nodes')}}    
    WHERE structure_type = 'Root'
),

learn_tree_states AS (
    SELECT
        started_at,
        user_id,
        root_node_id,
        head_content_node_id,
        status,
    FROM {{ ref('mart_learn_tree_states')}}    
),

learn_tree_unlocked_states AS (
    SELECT
        tree_unlocked_state_datetimestamp,
        user_id,
        root_node_id,
        learn_tree_unlocked_kind,
        license_id
    FROM {{ ref('mart_learn_tree_unlocked_states')}}    
),

summary AS (
  SELECT
    ts.user_id,
    ts.root_node_id,
    p.title AS course_title,
    ts.head_content_node_id AS current_node_id,
    ts.started_at AS module_started_at,
    ts.status AS module_progress_status,
    us.tree_unlocked_state_datetimestamp AS module_unlocked_datetimestamp,
    us.learn_tree_unlocked_kind,
    us.license_id,
  FROM learn_tree_states ts
  LEFT JOIN learn_tree_unlocked_states us USING (user_id, root_node_id)
  LEFT JOIN learn_tree_published_nodes p USING (root_node_id )
)

SELECT * FROM summary



