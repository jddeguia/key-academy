{% macro standardize_platform(
    collected_traffic_source_manual_source,
    manual_source,
    traffic_source_source
) %}

{% set facebook_regex = 'facebook|fb|m\\\\.facebook|lm\\\\.facebook|l\\\\.facebook' %}
{% set google_regex = 'adwords|googleads|doubleclick|youtube|google' %}
{% set bing_regex = 'bing' %}

CASE
    -- First prioritize collected_traffic_source_manual_source
    WHEN {{ collected_traffic_source_manual_source }} IS NOT NULL 
         AND {{ collected_traffic_source_manual_source }} != '(not set)' 
         AND {{ collected_traffic_source_manual_source }} != '{{site_source_name}}' THEN
        CASE
            WHEN REGEXP_CONTAINS(LOWER({{ collected_traffic_source_manual_source }}), r'{{ facebook_regex }}') THEN 'FACEBOOK'
            WHEN REGEXP_CONTAINS(LOWER({{ collected_traffic_source_manual_source }}), r'{{ google_regex }}') THEN 'GOOGLE'
            WHEN REGEXP_CONTAINS(LOWER({{ collected_traffic_source_manual_source }}), r'{{ bing_regex }}') THEN 'BING'
            ELSE 'OTHER'
        END
    
    -- Fallback to manual_source
    WHEN {{ manual_source }} IS NOT NULL AND {{ manual_source }} != '(not set)' THEN
        CASE
            WHEN REGEXP_CONTAINS(LOWER({{ manual_source }}), r'{{ facebook_regex }}') THEN 'FACEBOOK'
            WHEN REGEXP_CONTAINS(LOWER({{ manual_source }}), r'{{ google_regex }}') THEN 'GOOGLE'
            WHEN REGEXP_CONTAINS(LOWER({{ manual_source }}), r'{{ bing_regex }}') THEN 'BING'
            ELSE 'OTHER'
        END
    
    -- Final fallback to traffic_source_source
    WHEN {{ traffic_source_source }} IS NOT NULL AND {{ traffic_source_source }} != '(not set)' 
         AND {{ traffic_source_source }} != '{{source}}' AND {{ traffic_source_source }} != 'Data Not Available' THEN
        CASE
            WHEN REGEXP_CONTAINS(LOWER({{ traffic_source_source }}), r'{{ facebook_regex }}') THEN 'FACEBOOK'
            WHEN REGEXP_CONTAINS(LOWER({{ traffic_source_source }}), r'{{ google_regex }}') THEN 'GOOGLE'
            WHEN REGEXP_CONTAINS(LOWER({{ traffic_source_source }}), r'{{ bing_regex }}') THEN 'BING'
            ELSE 'OTHER'
        END
    
    -- Default for unmatched cases
    ELSE 'OTHER'
END

{% endmacro %}