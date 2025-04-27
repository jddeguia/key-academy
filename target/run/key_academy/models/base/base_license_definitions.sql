
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`base_license_definitions`
      
    
    

    OPTIONS()
    as (
      

WITH license_definition_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'LicenseDefinition:', '') AS license_definition_id, 
        _id AS id,
        JSON_QUERY(data, '$') AS data,
        JSON_QUERY(deletionInfo, '$') AS deletion_info,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM `intrepid-craft-450709-f9`.`test_orders_copy`.`license_definitions` 
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
        id,
        license_definition_id,
        SAFE.JSON_VALUE(data, '$.excludedNodeIds') AS excluded_node_ids,
        REPLACE(SAFE.JSON_VALUE(SAFE.JSON_QUERY(data, '$.includedNodeIds[0]')),"Node:",'') AS node_id,
        SAFE.JSON_VALUE(data, '$.kind') AS license_kind,
        SAFE.JSON_VALUE(data, '$.name') AS license_name,
        SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted, 
        SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_info,
    FROM license_definition_base
)

SELECT * FROM unnest_data
    );
  