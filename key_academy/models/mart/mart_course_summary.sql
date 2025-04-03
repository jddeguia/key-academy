{{
    config(
      materialized='table'
    )
}}

 WITH license_unlocked AS (
    SELECT 
        user_id,
        root_node_id AS node_id,
        license_id,
        tree_unlocked_state_datetimestamp AS course_purchased_at
    FROM {{ ref('mart_learn_tree_unlocked_states')}}   
 ),

 license_trial_started AS (
    SELECT 
        user_id,
        started_at AS trial_started_at,
        root_node_id AS node_id,
        status
    FROM {{ ref('mart_learn_tree_states')}}  
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, node_id ORDER BY started_at ASC) = 1
 ),

 license_definitions AS (
    SELECT
        id,
        license_definition_id,
        node_id,
        license_name AS title
    FROM {{ ref('mart_license_definitions')}}  
    WHERE license_name NOT LIKE '%InApp%'
 ),

course_summary AS (
    SELECT
        lu.user_id,
        lu.license_id,
        lu.node_id,
        ld.title,
        lu.course_purchased_at,
        ls.trial_started_at,
    FROM license_unlocked lu
    LEFT JOIN license_trial_started ls USING (user_id, node_id)
    LEFT JOIN license_definitions ld USING (node_id)
 )

SELECT *
FROM course_summary



