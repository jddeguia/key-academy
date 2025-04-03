{{
    config(
      materialized='table'
    )
}}

 WITH license_unlocked AS (
    SELECT 
      user_id,
      root_node_id,
      license_id,
      tree_unlocked_state_datetimestamp AS course_purchased_at
    FROM {{ ref('mart_learn_tree_unlocked_states')}}   
 ),

 license_trial_started AS (
    SELECT 
      user_id,
      started_at AS trial_started_at,
      root_node_id,
      status
    FROM {{ ref('mart_learn_tree_states')}}  
    WHERE status = 'Finished'
 ),

course_summary AS (
    SELECT
      lu.user_id,
      lu.license_id,
      lu.root_node_id,
      lu.course_purchased_at,
      ls.trial_started_at,
    FROM license_unlocked lu
    FULL JOIN license_trial_started ls USING (user_id, root_node_id)
    WHERE user_id IS NOT NULL
    AND course_purchased_at IS NOT NULL
 )

SELECT *
FROM course_summary



