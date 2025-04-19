{{
    config(
      materialized='table'
    )
}}

WITH certificate_base AS (
    SELECT
        can_order_at,
        ordered_at,
        certificate_id,
        verification_number,
        certificate_kind,
        recipient_id,
        performance_in_percent,
        received_in_node_id,
        received_in_root_node_id,
        is_with_honors,
        is_deleted,
        deletion_kind
    FROM {{ ref('base_learn_ihk_certificate_orders')}}   
)

SELECT * FROM certificate_base