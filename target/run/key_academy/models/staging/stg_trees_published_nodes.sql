
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`stg_trees_published_nodes`
      
    
    

    OPTIONS()
    as (
      

WITH base_trees_published_nodes AS (
    SELECT
        id,
        node_id,
        file_id,
        attachment_id,
        file_kind,
        file_title,
        draft_version,
        release_version,
        instructor_ids,
        all_child_refs_flat AS all_child,
        definition_type,
        content_kind,
        continue_config_type,
        continue_config_id,
        flow_config_type,
        flow_config_id,
        min_tries_show_answer,
        pass_config_type,
        pass_config_id,
        restart_if_passed_type,
        restart_if_passed_id,
        reward_type_kinds,
        reward_config_id,
        element_refs,
        next_content_node_id,
        previous_content_node_id,
        reward_type_ids,
        structure_type,
        title,
        next_sibling_ref,
        previous_sibling_ref,
        index_path,
        path_refs,
        is_deleted,
        deletion_kind
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_trees_published_nodes`   
)

SELECT * FROM base_trees_published_nodes
    );
  