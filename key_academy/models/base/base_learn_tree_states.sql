{{
    config(
      materialized='table'
    )
}}

WITH learn_tree_states_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'TreeState:', '') AS tree_state_id, 
        REPLACE(userId, 'User:', '') AS user_id, 
        _id AS id,
        startedAt AS started_at,
        JSON_QUERY(definition, '$') AS definition,
        REPLACE(rootNodeId, 'Node:', '') AS root_node_id,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM {{ source('test_orders', 'learn_tree_states')}}   
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
        started_at,
        user_id, 
        root_node_id,
        tree_state_id, 
        REPLACE(CAST(SAFE.JSON_VALUE(definition, '$.headContentNodeId') AS STRING), 'Node:', '') AS head_content_node_id,
        SAFE.JSON_VALUE(definition, '$.status') AS status        
    FROM learn_tree_states_base
)

SELECT * FROM unnest_data