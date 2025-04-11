{{
    config(
      materialized='table'
    )
}}

SELECT * FROM {{ ref('stg_learn_ihk_certificate_orders')}}   