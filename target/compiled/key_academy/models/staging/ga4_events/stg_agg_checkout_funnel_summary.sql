

WITH base AS (
    SELECT 
        *,
        -- New user if either:
        -- 1. It's a first_visit event, or
        -- 2. The event_date matches the user's first_seen_date (from user_first_touch_timestamp)
        CASE 
            WHEN event_name = 'first_visit' OR DATE(user_first_touch_timestamp) = event_date THEN 'new'
            WHEN (
                -- Original condition: user became active
                (is_active_user = TRUE AND LAG(is_active_user) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) = FALSE)
                -- OR time-based condition: visited more than 1 day after first touch
                OR DATE_DIFF(event_date, DATE(user_first_touch_timestamp), DAY) > 1
            ) THEN 'returning'
        END AS user_type,
        DATE(user_first_touch_timestamp) AS first_seen_date
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
    WHERE event_name IN ('first_visit', 'session_start', 'begin_checkout', 'add_payment_info', 'purchase')
),

summary AS (
    SELECT
        event_date,
        platform,
        user_type,
        SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) AS sessions,
        SUM(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
        SUM(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END) AS payment_details,
        SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS complete_checkout
    FROM base
    GROUP BY ALL
)

SELECT * FROM summary