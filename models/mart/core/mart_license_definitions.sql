{{
    config(
      materialized='table'
    )
}}

SELECT * FROM {{ ref('stg_license_definitions')}}   