{{
    config(
      materialized='table'
    )
}}

SELECT * FROM {{ ref('stg_trees_published_nodes')}}   