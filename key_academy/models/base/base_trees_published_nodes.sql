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
        REPLACE(JSON_VALUE(instructor_ids, '$[0]'), "Instructor:", "") AS first_instructor_id,
        REPLACE(JSON_VALUE(instructor_ids, '$[1]'), "Instructor:", "") AS second_instructor_id,
        SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted,
        SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_kind,
        type_definition
    FROM trees_published_nodes_base
)

SELECT * FROM unnest_data