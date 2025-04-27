

SELECT
  -- Event metadata
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
  TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  
  event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,

  -- Privacy info
  privacy_info.analytics_storage,
  privacy_info.ads_storage,
  privacy_info.uses_transient_token,

  -- User properties
  up.key AS user_properties_key,
  up.value.string_value AS user_properties_string_value,
  up.value.int_value AS user_properties_int_value,
  up.value.float_value AS user_properties_float_value,
  up.value.double_value AS user_properties_double_value,

  -- User LTV
  user_ltv.revenue,
  user_ltv.currency,
  
  -- User info
  event_name,
  user_id,
  user_pseudo_id,
  is_active_user,

  -- Device info
  device.category AS device_category,
  device.mobile_brand_name AS device_mobile_brand_name,
  device.mobile_model_name AS device_mobile_model_name,
  device.mobile_marketing_name AS device_mobile_marketing_name,
  device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
  device.operating_system AS device_operating_system,
  device.operating_system_version AS device_operating_system_version,
  device.vendor_id AS device_vendor_id,
  device.advertising_id AS device_advertising_id,
  device.language AS device_language,
  device.is_limited_ad_tracking AS device_is_limited_ad_tracking,
  device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
  device.browser AS device_browser,
  device.browser_version AS device_browser_version,
  device.web_info.browser AS device_web_info_browser,
  device.web_info.browser_version AS device_web_info_browser_version,
  device.web_info.hostname AS device_web_info_hostname,

  -- Location info
  geo.country AS country,
  geo.region AS region,
  geo.city AS city,
  geo.continent AS continent,
  geo.sub_continent AS sub_continent,
  geo.metro AS metro,
  
  -- app info metadata
  app_info.id AS app_info_id,
  app_info.version AS app_info_version,
  app_info.install_source AS app_install_source,
  app_info.firebase_app_id AS app_firebase_app_id,
  app_info.install_store AS app_install_store,

  -- traffic source metadata
  traffic_source.name AS traffic_source_name,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,

  -- collected traffic source metadata
  collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id,
  collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name,
  collected_traffic_source.manual_source AS collected_traffic_source_manual_source,
  collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium,
  collected_traffic_source.manual_term AS collected_traffic_source_manual_term,
  collected_traffic_source.manual_content AS collected_traffic_source_manual_content,
  collected_traffic_source.manual_source_platform AS collected_traffic_source_manual_source_platform,
  collected_traffic_source.manual_creative_format AS collected_traffic_source_manual_creative_format,
  collected_traffic_source.manual_marketing_tactic AS collected_traffic_source_manual_marketing_tactic,
  collected_traffic_source.gclid AS collected_traffic_source_gclid,
  collected_traffic_source.dclid AS collected_traffic_source_dclid,
  collected_traffic_source.srsltid AS collected_traffic_source_srsltid, 

  stream_id,
  platform,
  batch_event_index,
  batch_page_id,
  batch_ordering_id,

  event_dimensions.hostname AS event_dimensions_hostname,

  -- ecommerce metadata
  ecommerce.total_item_quantity,
  ecommerce.purchase_revenue_in_usd,
  ecommerce.purchase_revenue,
  ecommerce.refund_value_in_usd,
  ecommerce.refund_value,
  ecommerce.shipping_value_in_usd,
  ecommerce.shipping_value,
  ecommerce.tax_value_in_usd,
  ecommerce.tax_value,
  ecommerce.unique_items,
  ecommerce.transaction_id,

  -- Event parameters
  ep.key AS event_param_key,
  ep.value.string_value AS event_param_string_value,
  ep.value.int_value AS event_param_int_value,
  ep.value.float_value AS event_param_float_value,
  ep.value.double_value AS event_param_double_value,

  -- item metadata
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_variant,
  i.item_category,
  i.item_category2,
  i.item_category3,
  i.item_category4,
  i.item_category5,
  i.price_in_usd,
  i.price,
  i.quantity,
  i.item_revenue_in_usd,
  i.item_revenue,
  i.item_refund_in_usd,
  i.item_refund,
  i.coupon,
  i.affiliation,
  i.location_id,
  i.item_list_id,
  i.item_list_name,
  i.item_list_index,
  i.promotion_id,
  i.promotion_name,
  i.creative_name,
  i.creative_slot,
  
  -- Item parameters
  ip.key AS item_param_key,
  ip.value.string_value AS item_param_string_value,
  ip.value.int_value AS item_param_int_value,
  ip.value.float_value AS item_param_float_value,
  ip.value.double_value AS item_param_double_value,

  -- publisher
  publisher.ad_revenue_in_usd,
  publisher.ad_format,
  publisher.ad_source_name,
  publisher.ad_unit_id,

  -- Manual Campaign Fields
  session_traffic_source_last_click.manual_campaign.campaign_id AS manual_campaign_id,
  session_traffic_source_last_click.manual_campaign.campaign_name AS manual_campaign_name,
  session_traffic_source_last_click.manual_campaign.source AS manual_source,
  session_traffic_source_last_click.manual_campaign.medium AS manual_medium,
  session_traffic_source_last_click.manual_campaign.term AS manual_term,
  session_traffic_source_last_click.manual_campaign.content AS manual_content,
  session_traffic_source_last_click.manual_campaign.source_platform AS manual_source_platform,
  session_traffic_source_last_click.manual_campaign.creative_format AS manual_creative_format,
  session_traffic_source_last_click.manual_campaign.marketing_tactic AS manual_marketing_tactic,

  -- Google Ads Campaign Fields
  session_traffic_source_last_click.google_ads_campaign.customer_id AS google_ads_customer_id,
  session_traffic_source_last_click.google_ads_campaign.account_name AS google_ads_account_name,
  session_traffic_source_last_click.google_ads_campaign.campaign_id AS google_ads_campaign_id,
  session_traffic_source_last_click.google_ads_campaign.campaign_name AS google_ads_campaign_name,
  session_traffic_source_last_click.google_ads_campaign.ad_group_id AS google_ads_ad_group_id,
  session_traffic_source_last_click.google_ads_campaign.ad_group_name AS google_ads_ad_group_name,

  -- Cross Channel Campaign Fields
  session_traffic_source_last_click.cross_channel_campaign.campaign_id AS cross_channel_campaign_id,
  session_traffic_source_last_click.cross_channel_campaign.campaign_name AS cross_channel_campaign_name,
  session_traffic_source_last_click.cross_channel_campaign.source AS cross_channel_source,
  session_traffic_source_last_click.cross_channel_campaign.medium AS cross_channel_medium,
  session_traffic_source_last_click.cross_channel_campaign.source_platform AS cross_channel_source_platform,
  session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS cross_channel_default_group,
  session_traffic_source_last_click.cross_channel_campaign.primary_channel_group AS cross_channel_primary_group,

  -- SA360 Campaign Fields
  session_traffic_source_last_click.sa360_campaign.campaign_id AS sa360_campaign_id,
  session_traffic_source_last_click.sa360_campaign.campaign_name AS sa360_campaign_name,
  session_traffic_source_last_click.sa360_campaign.source AS sa360_source,
  session_traffic_source_last_click.sa360_campaign.medium AS sa360_medium,
  session_traffic_source_last_click.sa360_campaign.ad_group_id AS sa360_ad_group_id,
  session_traffic_source_last_click.sa360_campaign.ad_group_name AS sa360_ad_group_name,
  session_traffic_source_last_click.sa360_campaign.creative_format AS sa360_creative_format,
  session_traffic_source_last_click.sa360_campaign.engine_account_name AS sa360_engine_account_name,
  session_traffic_source_last_click.sa360_campaign.engine_account_type AS sa360_engine_account_type,
  session_traffic_source_last_click.sa360_campaign.manager_account_name AS sa360_manager_account_name,

   -- CM360 (Campaign Manager 360) Campaign Fields
  session_traffic_source_last_click.cm360_campaign.campaign_id AS cm360_campaign_id,
  session_traffic_source_last_click.cm360_campaign.campaign_name AS cm360_campaign_name,
  session_traffic_source_last_click.cm360_campaign.source AS cm360_source,
  session_traffic_source_last_click.cm360_campaign.medium AS cm360_medium,
  session_traffic_source_last_click.cm360_campaign.account_id AS cm360_account_id,
  session_traffic_source_last_click.cm360_campaign.account_name AS cm360_account_name,
  session_traffic_source_last_click.cm360_campaign.advertiser_id AS cm360_advertiser_id,
  session_traffic_source_last_click.cm360_campaign.advertiser_name AS cm360_advertiser_name,
  session_traffic_source_last_click.cm360_campaign.creative_id AS cm360_creative_id,
  session_traffic_source_last_click.cm360_campaign.creative_format AS cm360_creative_format,
  session_traffic_source_last_click.cm360_campaign.creative_name AS cm360_creative_name,
  session_traffic_source_last_click.cm360_campaign.creative_type AS cm360_creative_type,
  session_traffic_source_last_click.cm360_campaign.creative_type_id AS cm360_creative_type_id,
  session_traffic_source_last_click.cm360_campaign.creative_version AS cm360_creative_version,
  session_traffic_source_last_click.cm360_campaign.placement_id AS cm360_placement_id,
  session_traffic_source_last_click.cm360_campaign.placement_cost_structure AS cm360_placement_cost_structure,
  session_traffic_source_last_click.cm360_campaign.placement_name AS cm360_placement_name,
  session_traffic_source_last_click.cm360_campaign.rendering_id AS cm360_rendering_id,
  session_traffic_source_last_click.cm360_campaign.site_id AS cm360_site_id,
  session_traffic_source_last_click.cm360_campaign.site_name AS cm360_site_name,

  -- DV360 (Display & Video 360) Campaign Fields
  session_traffic_source_last_click.dv360_campaign.campaign_id AS dv360_campaign_id,
  session_traffic_source_last_click.dv360_campaign.campaign_name AS dv360_campaign_name,
  session_traffic_source_last_click.dv360_campaign.source AS dv360_source,
  session_traffic_source_last_click.dv360_campaign.medium AS dv360_medium,
  session_traffic_source_last_click.dv360_campaign.advertiser_id AS dv360_advertiser_id,
  session_traffic_source_last_click.dv360_campaign.advertiser_name AS dv360_advertiser_name,
  session_traffic_source_last_click.dv360_campaign.creative_id AS dv360_creative_id,
  session_traffic_source_last_click.dv360_campaign.creative_format AS dv360_creative_format,
  session_traffic_source_last_click.dv360_campaign.creative_name AS dv360_creative_name,
  session_traffic_source_last_click.dv360_campaign.exchange_id AS dv360_exchange_id,
  session_traffic_source_last_click.dv360_campaign.exchange_name AS dv360_exchange_name,
  session_traffic_source_last_click.dv360_campaign.insertion_order_id AS dv360_insertion_order_id,
  session_traffic_source_last_click.dv360_campaign.insertion_order_name AS dv360_insertion_order_name,
  session_traffic_source_last_click.dv360_campaign.line_item_id AS dv360_line_item_id,
  session_traffic_source_last_click.dv360_campaign.line_item_name AS dv360_line_item_name,
  session_traffic_source_last_click.dv360_campaign.partner_id AS dv360_partner_id,
  session_traffic_source_last_click.dv360_campaign.partner_name AS dv360_partner_name,


FROM `intrepid-craft-450709-f9`.`ga4_copy`.`events_*`
LEFT JOIN UNNEST(event_params) AS ep
LEFT JOIN UNNEST(user_properties) AS up
LEFT JOIN UNNEST(items) AS i  
LEFT JOIN UNNEST(i.item_params) AS ip