

WITH user_events_with_type AS (
  SELECT
    user_pseudo_id,
    event_date,
    platform,
    event_name,
    user_first_touch_timestamp,
    event_timestamp,
    is_active_user,
    -- First calculate user_type at the row level
    CASE
      WHEN event_name = 'first_visit' OR DATE(user_first_touch_timestamp) = event_date THEN 'new'
      WHEN (
        is_active_user = TRUE AND 
        LAG(is_active_user) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) = FALSE
      ) THEN 'returning'
      WHEN DATE_DIFF(event_date, DATE(user_first_touch_timestamp), DAY) > 1 THEN 'returning'
      ELSE NULL
    END AS user_type
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
  WHERE event_name IN ('first_visit', 'session_start', 'begin_checkout', 'add_payment_info', 'purchase')
),

filtered_events AS (
  SELECT * FROM user_events_with_type
  WHERE user_type IS NOT NULL
),

user_journeys AS (
  SELECT
    user_pseudo_id,
    event_date,
    platform,
    user_type,
    -- Get the first timestamp for each event type per user per day
    MAX(CASE WHEN event_name = 'session_start' THEN event_timestamp END) AS session_start_time,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN event_timestamp END) AS begin_checkout_time,
    MAX(CASE WHEN event_name = 'add_payment_info' THEN event_timestamp END) AS payment_details_time,
    MAX(CASE WHEN event_name = 'purchase' THEN event_timestamp END) AS purchase_time
  FROM filtered_events
  GROUP BY ALL
),

funnel_metrics AS (
  SELECT
    event_date,
    platform,
    user_type,
    -- Sessions (no sequence requirement)
    SUM(CASE WHEN session_start_time IS NOT NULL THEN 1 ELSE 0 END) AS sessions,
    -- Begin checkout must happen after session start
    SUM(CASE 
          WHEN begin_checkout_time IS NOT NULL AND 
               (session_start_time IS NULL OR begin_checkout_time >= session_start_time)
          THEN 1 ELSE 0 
        END) AS begin_checkout,
    -- Payment details must happen after begin checkout
    SUM(CASE 
          WHEN payment_details_time IS NOT NULL AND 
               (begin_checkout_time IS NULL OR payment_details_time >= begin_checkout_time)
          THEN 1 ELSE 0 
        END) AS payment_details,
    -- Purchase must happen after payment details
    SUM(CASE 
          WHEN purchase_time IS NOT NULL AND 
               (payment_details_time IS NULL OR purchase_time >= payment_details_time)
          THEN 1 ELSE 0 
        END) AS complete_checkout
  FROM user_journeys
  GROUP BY ALL
)

SELECT * FROM funnel_metrics