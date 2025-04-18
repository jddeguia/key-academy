{{
    config(
      materialized='table'
    )
}}

WITH learn_tree_unlocked_states_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'TreeUnlockedState:', '') AS tree_unlocked_state_id, 
        _id AS id,
        JSON_QUERY(data, '$') AS data,
        lt.when AS tree_unlocked_state_datetimestamp,
        REPLACE(rootId, 'Node:', '') AS root_node_id,
        REPLACE(userId, 'User:', '') AS user_id, 
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,        
    FROM {{ source('test_orders', 'learn_tree_unlocked_states')}} AS lt
),

unnest_data AS (
    SELECT
        airbyte_extracted_at,
        airbyte_generation_id,
        ab_cdc_deleted_at,
        ab_cdc_updated_at,      
        airbyte_raw_id,
        ab_cdc_cursor,
        SAFE.JSON_VALUE(airbyte_meta, '$.changes') AS airbyte_changes, 
        SAFE.JSON_VALUE(airbyte_meta, '$.sync_id') AS airbyte_sync_id, 
        tree_unlocked_state_datetimestamp,
        id,
        tree_unlocked_state_id, 
        user_id, 
        root_node_id,
        SAFE.JSON_VALUE(data, '$.kind') AS learn_tree_unlocked_kind, 
        REPLACE(SAFE.JSON_VALUE(data, '$.licenseId'), "License:", '') AS license_id
    FROM learn_tree_unlocked_states_base
)

SELECT * FROM unnest_data