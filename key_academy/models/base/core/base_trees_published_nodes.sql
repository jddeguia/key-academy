{{
    config(
      materialized='table'
    )
}}

WITH trees_published_nodes_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'Node:', '') AS node_id, 
        _id AS id,
        REPLACE(imageRef, 'FileDescription:', '') as file_description_id,
        JSON_QUERY(versioning, '$') AS versioning,
        JSON_QUERY(attachments, '$') AS attachments,
        JSON_QUERY(deletionInfo, '$') AS deletion_info,
        JSON_QUERY(instructorIds, '$') AS instructor_ids,
        JSON_QUERY(typeDefinition, '$') AS type_definition,
        JSON_QUERY(releaseCoordinates, '$') AS release_coordinates,
        JSON_QUERY(structureDefinition, '$') AS structure_definition,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM {{ source('test_orders', 'trees_published_nodes')}}  
),

--not done
unnest_data AS (
    SELECT
        airbyte_extracted_at,
        airbyte_generation_id,
        ab_cdc_deleted_at,
        ab_cdc_updated_at,      
        airbyte_raw_id,
        ab_cdc_cursor,
        file_description_id,
        id,
        node_id,
        SAFE.JSON_VALUE(attachments, '$.fileId') AS file_id,
        SAFE.JSON_VALUE(attachments, '$.id') AS attachment_id,
        SAFE.JSON_VALUE(attachments, '$.kind') AS file_kind,
        SAFE.JSON_VALUE(attachments, '$.title') AS file_title,
        SAFE.JSON_VALUE(versioning, '$.draftVersion') AS draft_version,
        SAFE.JSON_VALUE(versioning, '$.releaseVersion') AS release_version,
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(instructor_ids),r'"Instructor:([^"]+)"',r'\1'),r'[\[\]"]','') AS instructor_ids,
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(type_definition['childRefs']),r'"Node:([^"]+)"',r'\1'),  r'[\[\]"]','') AS all_child_refs_flat,
        SAFE.JSON_VALUE(type_definition, '$.definitionType') AS definition_type,
        SAFE.JSON_VALUE(type_definition, '$.contentKind') AS content_kind,
        SAFE.JSON_VALUE(type_definition, '$.continueConfig.configType') AS continue_config_type,
        REPLACE(SAFE.JSON_VALUE(type_definition, '$.continueConfig.id'), 'ContinueContentConfig:', '') AS continue_config_id,
        SAFE.JSON_VALUE(type_definition, '$.flowConfig.configType') AS flow_config_type,
        REPLACE(SAFE.JSON_VALUE(type_definition, '$.flowConfig.id'), 'FlowELearningContentConfig:', '') AS flow_config_id,
        SAFE.JSON_VALUE(type_definition, '$.flowConfig.minNumTriesTillShowAnswer') AS min_tries_show_answer,
        SAFE.JSON_VALUE(type_definition, '$.passConfig.configType') AS pass_config_type,
        REPLACE(SAFE.JSON_VALUE(type_definition, '$.passConfig.id'), 'PassContentConfig:', '') AS pass_config_id,
        SAFE.JSON_VALUE(type_definition, '$.restartIfPassedConfig.configType') AS restart_if_passed_type,
        REPLACE(SAFE.JSON_VALUE(type_definition, '$.restartIfPassedConfig.id'), 'RestartIfPassedContentConfig:', '') AS restart_if_passed_id,
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(SAFE.JSON_EXTRACT(type_definition, '$.rewardTypeConfigs')), r'\{"id":"Reward:[^"]+","kind":"([^"]+)"\}', r'\1'), r'[\[\]"]', '') AS reward_type_kinds,

        REPLACE(SAFE.JSON_VALUE(type_definition, '$.rewardConfig.id'), 'RewardContentConfig:', '') AS reward_config_id,
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(type_definition['elementRefs']),r'"Element:([^"]+)"',r'\1'),  r'[\[\]"]','') AS element_refs,        
        SAFE.JSON_VALUE(release_coordinates, '$.next_content_node_id') AS next_content_node_id,
        SAFE.JSON_VALUE(release_coordinates, '$.previous_content_node_id') AS previous_content_node_id,
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(SAFE.JSON_VALUE(type_definition, '$.rewardTypeConfigs')), r'"Reward:([^"]+)"', r'\1'), r'[\[\]"]', '') AS reward_type_ids,
        
        SAFE.JSON_VALUE(structure_definition, '$.structureType') AS structure_type,
        SAFE.JSON_VALUE(structure_definition, '$.title') AS title,

        -- coordinates.*
        SAFE.JSON_VALUE(structure_definition, '$.coordinates.nextSiblingRef') AS next_sibling_ref,
        SAFE.JSON_VALUE(structure_definition, '$.coordinates.previousSiblingRef') AS previous_sibling_ref,

        -- Flattening array values from coordinates
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(SAFE.JSON_EXTRACT(structure_definition, '$.coordinates.indexPath')), r'[\[\]"]', ''), r',', '-') AS index_path, -- returns "0-2"
        REGEXP_REPLACE(REGEXP_REPLACE(TO_JSON_STRING(SAFE.JSON_EXTRACT(structure_definition, '$.coordinates.pathRefs')), r'"Node:([^"]+)"', r'\1'), r'[\[\]"]', '') AS path_refs,

        SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted,
        SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_kind
    FROM trees_published_nodes_base
)

SELECT * FROM unnest_data