

SELECT
  event_timestamp,
  event_date,
  COALESCE(user_id, user_pseudo_id) AS user_id,
  user_pseudo_id,
  event_name,
  
  -- CM360 Campaign Fields
  cm360_campaign_id AS campaign_id,
  cm360_campaign_name AS campaign_name,
  cm360_source AS source,
  cm360_medium AS medium,
  cm360_account_id AS account_id,
  cm360_account_name AS account_name,
  cm360_advertiser_id AS advertiser_id,
  cm360_advertiser_name AS advertiser_name,
  cm360_creative_id AS creative_id,
  cm360_creative_format AS creative_format,
  cm360_creative_name AS creative_name,
  cm360_creative_type AS creative_type,
  cm360_creative_type_id AS creative_type_id,
  cm360_creative_version AS creative_version,
  cm360_placement_id AS placement_id,
  cm360_placement_cost_structure AS placement_cost_structure,
  cm360_placement_name AS placement_name,
  cm360_rendering_id AS rendering_id,
  cm360_site_id AS site_id,
  cm360_site_name AS site_name,

  -- Additional dimensions
  stream_id,
  platform,
  country,
  device_category,

  -- Metrics
  purchase_revenue_in_usd,
  ad_revenue_in_usd

FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
WHERE cm360_campaign_id IS NOT NULL