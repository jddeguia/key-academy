

WITH registration_events AS (
  SELECT
    user_pseudo_id,
    event_date,
    platform,
    event_name,
    event_timestamp,
    -- Create a unique session identifier
    CASE WHEN event_name = 'session_start' 
         THEN CONCAT(user_pseudo_id, CAST(event_timestamp AS STRING)) 
         ELSE NULL END AS session_id
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_ga4_events`
  WHERE event_name IN ('session_start', 'sign_up', 'sign_up_request', 'login')
),

user_journeys AS (
  SELECT
    user_pseudo_id,
    event_date,
    platform,
    -- Get the first timestamp for each event type per user per day
    MAX(CASE WHEN event_name = 'session_start' THEN event_timestamp END) AS session_start_time,
    MAX(CASE WHEN event_name = 'sign_up' THEN event_timestamp END) AS sign_up_time,
    MAX(CASE WHEN event_name = 'sign_up_request' THEN event_timestamp END) AS sign_up_request_time,
    MAX(CASE WHEN event_name = 'login' THEN event_timestamp END) AS login_time,
    -- Get any session ID (we'll use this for counting)
    MAX(session_id) AS session_id
  FROM registration_events
  GROUP BY ALL
),

funnel_metrics AS (
  SELECT
    event_date,
    platform,
    -- Count sessions using SUM CASE WHEN
    SUM(CASE WHEN session_start_time IS NOT NULL THEN 1 ELSE 0 END) AS sessions,
    -- Sign up must happen after session start
    SUM(CASE 
          WHEN sign_up_time IS NOT NULL AND 
               (session_start_time IS NULL OR sign_up_time >= session_start_time)
          THEN 1 ELSE 0 
        END) AS create_account,
    -- Sign up request must happen after sign up
    SUM(CASE 
          WHEN sign_up_request_time IS NOT NULL AND 
               (sign_up_time IS NULL OR sign_up_request_time >= sign_up_time)
          THEN 1 ELSE 0 
        END) AS create_password,
    -- Login must happen after sign up request
    SUM(CASE 
          WHEN login_time IS NOT NULL AND 
               (sign_up_request_time IS NULL OR login_time >= sign_up_request_time)
          THEN 1 ELSE 0 
        END) AS confirm_email,
  FROM user_journeys
  GROUP BY ALL
)

SELECT 
  event_date,
  platform,
  sessions,
  create_account,
  create_password,
  confirm_email
FROM funnel_metrics