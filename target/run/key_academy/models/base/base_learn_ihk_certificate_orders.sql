
  
    

    create or replace table `intrepid-craft-450709-f9`.`key_academy`.`base_learn_ihk_certificate_orders`
      
    
    

    OPTIONS()
    as (
      



WITH raw_certificate_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'IHKCertificateOrder:', '') AS certificate_id, 
        kind,
        JSON_QUERY(userData, '$') AS user_data,
        JSON_QUERY(initialData, '$') AS initial_data,
        JSON_QUERY(deletionInfo, '$') AS deletion_info,
        JSON_QUERY(certificateData, '$') AS certificate_data,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM `intrepid-craft-450709-f9`.`test_orders_copy`.`learn_ihk_certificate_orders`  
),

unnest_data AS (
    SELECT
        airbyte_extracted_at,
        airbyte_generation_id,
        ab_cdc_deleted_at,
        ab_cdc_updated_at,      
        airbyte_raw_id,
        ab_cdc_cursor,
        certificate_id,
        kind AS certificate_kind,
        SAFE.JSON_VALUE(initial_data, '$.canOrderAt') AS can_order_at,
        REPLACE(SAFE.JSON_VALUE(initial_data, '$.recipientId'), 'User:', '') AS recipient_id,
        ROUND(CAST(SAFE.JSON_VALUE(initial_data, '$.performanceInPercent') AS FLOAT64), 2) AS performance_in_percent,
        REPLACE(SAFE.JSON_VALUE(initial_data, '$.receivedInContentId'), 'Node:', '') AS received_in_node_id,
        REPLACE(SAFE.JSON_VALUE(initial_data, '$.receivedInRootId'), 'Node:', '') AS received_in_root_node_id,
        SAFE.JSON_VALUE(initial_data, '$.isWithHonors') AS is_with_honors,
        SAFE.JSON_VALUE(deletion_info, '$.isDeleted') AS is_deleted,
        SAFE.JSON_VALUE(deletion_info, '$.kind') AS deletion_kind,
        SAFE.JSON_VALUE(certificate_data, '$.orderedAt') AS ordered_at,
        SAFE.JSON_VALUE(certificate_data, '$.verificationNumber') AS verification_number,
    FROM raw_certificate_base
)

SELECT * FROM unnest_data
    );
  