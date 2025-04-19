{{
    config(
      materialized='table'
    )
}}

WITH base_license_definitions AS (
    SELECT
        id,
        license_definition_id,
        node_id,
        license_kind,
        license_name,
        is_deleted
    FROM {{ ref('base_license_definitions')}}   
)

SELECT * FROM base_license_definitions