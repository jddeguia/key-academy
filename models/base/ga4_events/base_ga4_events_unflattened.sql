{{
  config(
    materialized = 'view'
  )
}}

WITH base_data AS (
  SELECT
    FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', CAST(event_date AS STRING))) AS event_date,
    event_name,
    platform AS location,
    (
      SELECT STRING_AGG(
        FORMAT('"%s": %s', 
          param.key,
          CASE
            WHEN param.value.string_value IS NOT NULL THEN FORMAT('"%s"', param.value.string_value)
            WHEN param.value.int_value IS NOT NULL THEN CAST(param.value.int_value AS STRING)
            WHEN param.value.float_value IS NOT NULL THEN CAST(param.value.float_value AS STRING)
            WHEN param.value.double_value IS NOT NULL THEN CAST(param.value.double_value AS STRING)
            ELSE 'null'
          END
        ),
        ', '
      )
      FROM UNNEST(event_params) AS param
    ) AS event_params_json_raw
  FROM {{ source('google_analytics', 'events_*') }}
)

SELECT
  event_date,
  event_name,
  location,
  CONCAT('{', event_params_json_raw, '}') AS event_params_json
FROM base_data