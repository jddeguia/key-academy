

WITH license_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'Product:', '') AS product_id, 
        _id AS id,
        JSON_QUERY(data, '$') AS data,
        title,
        isHidden AS is_hidden,
        netPrice AS net_price,
        isTaxFree AS is_tax_free,
        JSON_QUERY(extensions, '$') AS extensions,
        JSON_QUERY(deletionInfo, '$') AS deletion_info,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM `intrepid-craft-450709-f9`.`test_orders_copy`.`billing_products`  
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
        product_id,
        title,
        is_hidden,
        net_price,
        is_tax_free,
        SAFE.JSON_VALUE(data, '$.kind') AS license_kind, 
        REPLACE(SAFE.JSON_VALUE(data, '$.licenseDefinition'), "LicenseDefinition:",'') AS license_definition_id,
        SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted, 
        SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_info,
    FROM license_base
)

SELECT * FROM unnest_data