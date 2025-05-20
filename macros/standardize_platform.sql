{% macro standardize_platform(traffic_source_source) %}

{% set platform_patterns = {
    'FACEBOOK': [
        '^(?i)(fb|facebook|ig|insta)$',
        '^(?i)(l|m|lm)\.facebook\.com$',
        '^(?i)adsmanager\.facebook',
        '^(?i)facebook\.com$'
    ],
    'GOOGLE': [
        '^(?i)(google|adwords|google-play)$',
        '^(?i)(youtube\.com|google\.com)$',
        '^(?i)(m\.youtube\.com|tagassistant\.google)$'
    ],
    'BING': [
        '^(?i)bing$',
        '^(?i)cn\.bing\.com$'
    ]
} %}

CASE
    WHEN {{ traffic_source_source }} IS NULL 
         OR {{ traffic_source_source }} IN ('(not set)', '{{source}}', 'Data Not Available', '(direct)', 'data not available') 
    THEN 'OTHER'
    
    {% for platform, patterns in platform_patterns.items() %}
    {% for pattern in patterns %}
    WHEN REGEXP_CONTAINS({{ traffic_source_source }}, r'{{ pattern }}') THEN '{{ platform }}'
    {% endfor %}
    {% endfor %}
    
    ELSE 'OTHER'
END

{% endmacro %}