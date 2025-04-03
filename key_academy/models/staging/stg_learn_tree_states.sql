{{
    config(
      materialized='table'
    )
}}

WITH base_learn_tree_states AS (
    SELECT
        started_at,
        user_id,
        root_node_id,
        tree_state_id,
        head_content_node_id,
        status
    FROM {{ ref('base_learn_tree_states')}}   
)

SELECT * FROM base_learn_tree_states