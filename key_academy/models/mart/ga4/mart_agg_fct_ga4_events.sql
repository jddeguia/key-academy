{{
  config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by = ["event_name"]
  )
}}

{% set locations = {
  "WEB": "WEB",
  "ANDROID": "ANDROID", 
  "IOS": "IOS"
} %}

WITH base AS (
  SELECT 
    event_date,
    event_name,
    location,
    COUNT(*) as event_count
  FROM {{ ref('mart_fct_ga4_events') }}
  GROUP BY ALL
)

SELECT 
  event_date,
  event_name,
  {% for location_key, location_alias in locations.items() %}
  SUM(CASE WHEN location = '{{ location_key }}' THEN event_count ELSE 0 END) AS {{ location_alias }}{% if not loop.last %},{% endif %}
  {% endfor %}
FROM base
GROUP BY ALL