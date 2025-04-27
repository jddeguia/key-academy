

WITH auth_users_base AS (
    SELECT
        _airbyte_raw_id AS airbyte_raw_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        JSON_QUERY(_airbyte_meta, '$') AS airbyte_meta,
        _airbyte_generation_id AS airbyte_generation_id,
        REPLACE(id, 'User:', '') AS user_id, 
        name,
        email,
        activated AS is_activated,
        isDeleted AS is_deleted,
        JSON_QUERY(extensions, '$') AS extensions,
        JSON_QUERY(groupAssociations, '$') AS group_associations,
        registeredAT AS registered_at,
        _ab_cdc_cursor AS ab_cdc_cursor,
        _ab_cdc_deleted_at AS ab_cdc_deleted_at,
        _ab_cdc_updated_at AS ab_cdc_updated_at,
    FROM `intrepid-craft-450709-f9`.`test_orders_copy`.`auth_users`  
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
        user_id, 
        registered_at,
        SAFE.JSON_VALUE(extensions, '$.firstName') AS first_name,
        SAFE.JSON_VALUE(extensions, '$.lastName') AS last_name,
        name,
        email,
        is_activated,
        is_deleted,
        SAFE.JSON_VALUE(extensions, '$.firstLogin') AS is_first_login,
        REPLACE(SAFE.JSON_VALUE(group_associations, '$.accountRef'), "Account:", '') AS account_ref_id, 
        SAFE.JSON_VALUE(group_associations, '$.groupRef') AS group_ref,
        SAFE.JSON_VALUE(extensions, '$.adsOptIn') AS ads_opt_in,
        SAFE.JSON_VALUE(extensions, '$.branch') AS branch,
        SAFE.JSON_VALUE(extensions, '$.gamificationPoints') AS gamification_points,
        SAFE.JSON_VALUE(extensions, '$.kind') AS kind,
        SAFE.JSON_VALUE(extensions, '$.position') AS position,
        SAFE.JSON_VALUE(extensions, '$.teamSize') AS team_size,
        REPLACE(SAFE.JSON_VALUE(extensions, '$.usedLastAccountId'), "Account:", '') AS used_last_account_id,
        SAFE.JSON_VALUE(extensions, '$.gamification.level') AS gamification_level,
        SAFE.JSON_VALUE(extensions, '$.potentialAnalysisMaxRewardAcknowledged') AS is_max_reward_acknowledged
    FROM auth_users_base
)

SELECT * FROM unnest_data