
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_learn_tree_unlocked_states`
      
    
    

    OPTIONS()
    as (
      

WITH base_learn_tree_unlocked_states AS (
    SELECT
        tree_unlocked_state_datetimestamp,
        id,
        tree_unlocked_state_id,
        user_id,
        root_node_id,
        learn_tree_unlocked_kind,
        license_id
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_learn_tree_unlocked_states`   
)

SELECT * FROM base_learn_tree_unlocked_states
    );
  