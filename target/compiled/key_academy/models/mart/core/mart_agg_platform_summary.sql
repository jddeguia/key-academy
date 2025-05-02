

WITH revenue AS (
    SELECT
        DATE(created_at) AS date,
        ROUND(SUM(
        CASE
            WHEN selected_payment_kind = 'OneTime' THEN SAFE_CAST(cart_including_all_discounts_gross_price AS FLOAT64)
            WHEN selected_payment_kind = 'Monthly' THEN SAFE_CAST(total_amount_gross AS FLOAT64)
            ELSE 0
        END
    ), 2) AS revenue
  FROM `intrepid-craft-450709-f9`.`key_academy`.`base_billing_orders`
  GROUP BY ALL
),

courses_progress AS (
    SELECT
        module_started_date AS date,
        SUM(trials) AS trials,
        SUM(lessons_completed) AS lessons_completed,
        SUM(certificates) AS certificates
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_agg_course_progress`
    GROUP BY ALL
),

licenses_sold AS (
    SELECT
        DATE(module_started_at) AS date,
        SUM(CASE WHEN license_id IS NOT NULL THEN 1 ELSE 0 END) AS licenses_sold
    FROM `intrepid-craft-450709-f9`.`key_academy`.`mart_user_module_progress`
    GROUP BY ALL
),

registrations AS (
    SELECT
        DATE(registered_at) AS date,
        COUNT(*) AS registrations
    FROM `intrepid-craft-450709-f9`.`key_academy`.`base_auth_users`    
    GROUP BY ALL
),

logins AS (
    SELECT
        event_date AS date,
        SUM(CASE WHEN event_name = 'login' THEN 1 ELSE 0 END) AS logins,
        SUM(CASE WHEN is_active_user IS TRUE THEN 1 ELSE 0 END) AS active_users,    
    FROM `intrepid-craft-450709-f9`.`key_academy`.`stg_ga4_account_events`
    GROUP BY ALL
),

summary AS (
    SELECT 
        date,
        reg.registrations,
        c.trials,
        l.licenses_sold,
        r.revenue/10 AS revenue,
        log.logins,
        log.active_users,
        c.lessons_completed,
        c.certificates      
    FROM logins log
    FULL JOIN courses_progress c USING (date)
    FULL JOIN licenses_sold l USING (date)
    FULL JOIN registrations reg USING (date)
    FULL JOIN revenue r USING (date)
)

SELECT * 
FROM summary
ORDER BY date DESC