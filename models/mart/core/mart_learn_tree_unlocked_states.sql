{{
    config(
      materialized='table'
    )
}}

SELECT * FROM {{ ref('stg_learn_tree_unlocked_states')}}   