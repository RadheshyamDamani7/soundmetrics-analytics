-- ============================================================
--  SoundMetrics — Phase 4: SQL Analysis
--  File        : 03_business_queries.sql
--  Description : 15 real business questions answered with SQL
--  Run this    : Open in MySQL Workbench, run queries one by one
--                Select a query block and press Ctrl+Shift+Enter
-- ============================================================

USE soundmetrics;

-- ============================================================
-- QUERY 01: How many total users do we have per country?
-- Business use: Regional team sizing and localisation priority
-- ============================================================
-- CONCEPT: GROUP BY groups all rows with the same country together
--          COUNT(*) counts how many rows are in each group
--          ORDER BY DESC sorts from largest to smallest

SELECT
    country,
    COUNT(*)                                AS total_users,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)
                                            AS pct_of_total
FROM users
GROUP BY country
ORDER BY total_users DESC
LIMIT 15;


-- ============================================================
-- QUERY 02: What is our plan distribution?
-- Business use: Understand free vs paid ratio (freemium health)
-- ============================================================
-- CONCEPT: CASE WHEN is like an if/else statement in SQL
--          We use it to group Free vs all paid plans

SELECT
    plan_type,
    COUNT(*)                                AS total_users,
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM subscriptions), 2)
                                            AS percentage,
    CASE
        WHEN plan_type = 'Free'  THEN 'Free Tier'
        ELSE                          'Paid Tier'
    END                                     AS tier
FROM subscriptions
WHERE plan_type IS NOT NULL
GROUP BY plan_type
ORDER BY total_users DESC;


-- ============================================================
-- QUERY 03: Monthly new user signups (growth trend)
-- Business use: Track if the business is growing month on month
-- ============================================================
-- CONCEPT: DATE_FORMAT extracts year+month from a date
--          This lets us group all signups in the same month together

SELECT
    DATE_FORMAT(signup_date, '%Y-%m')       AS signup_month,
    COUNT(*)                                AS new_users,
    SUM(COUNT(*)) OVER (
        ORDER BY DATE_FORMAT(signup_date, '%Y-%m')
    )                                       AS cumulative_users
FROM users
WHERE signup_date IS NOT NULL
GROUP BY signup_month
ORDER BY signup_month;


-- ============================================================
-- QUERY 04: Which acquisition channel brings the most users?
--           And what % of those users are on paid plans?
-- Business use: Decide where to invest marketing budget
-- ============================================================
-- CONCEPT: JOIN connects two tables using a shared column (user_id)
--          LEFT JOIN keeps all users even if they have no subscription

SELECT
    u.acquisition_channel,
    COUNT(DISTINCT u.user_id)               AS total_users,
    SUM(CASE WHEN s.plan_type != 'Free'
             AND s.plan_type IS NOT NULL
             THEN 1 ELSE 0 END)             AS paid_users,
    ROUND(
        SUM(CASE WHEN s.plan_type != 'Free'
                 AND s.plan_type IS NOT NULL
                 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(DISTINCT u.user_id)
    , 2)                                    AS paid_conversion_pct
FROM users u
LEFT JOIN subscriptions s ON u.user_id = s.user_id
GROUP BY u.acquisition_channel
ORDER BY paid_conversion_pct DESC;


-- ============================================================
-- QUERY 05: What are the top 10 most played genres?
--           Include skip rate and average listen duration.
-- Business use: Content licensing and playlist curation decisions
-- ============================================================
-- CONCEPT: AVG() calculates the average of a column
--          We multiply is_skip (0 or 1) average by 100 to get a %

SELECT
    genre,
    COUNT(*)                                AS total_plays,
    ROUND(AVG(listen_duration_sec), 1)      AS avg_duration_sec,
    ROUND(AVG(is_skip) * 100, 2)           AS skip_rate_pct,
    ROUND(AVG(is_repeat_listen) * 100, 2)  AS repeat_rate_pct
FROM listening_events
WHERE genre != 'Unknown'
  AND genre IS NOT NULL
GROUP BY genre
ORDER BY total_plays DESC
LIMIT 10;


-- ============================================================
-- QUERY 06: What device do users listen on the most?
-- Business use: Product team knows where to invest engineering effort
-- ============================================================
SELECT
    device,
    COUNT(*)                                AS total_plays,
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM listening_events
           WHERE device != 'Unknown'), 2)   AS pct_of_plays,
    ROUND(AVG(listen_duration_sec), 1)      AS avg_duration_sec
FROM listening_events
WHERE device != 'Unknown'
  AND device IS NOT NULL
GROUP BY device
ORDER BY total_plays DESC;


-- ============================================================
-- QUERY 07: What hour of the day sees the most listening?
-- Business use: Server capacity planning and content scheduling
-- ============================================================
-- CONCEPT: HOUR() extracts the hour from a TIME column
--          LPAD pads single-digit hours with a 0 (e.g. 9 → '09')

SELECT
    HOUR(event_time)                        AS hour_of_day,
    CONCAT(LPAD(HOUR(event_time), 2, '0'),
           ':00')                           AS hour_label,
    COUNT(*)                                AS total_plays,
    ROUND(AVG(listen_duration_sec), 1)      AS avg_duration_sec
FROM listening_events
WHERE event_time IS NOT NULL
GROUP BY HOUR(event_time)
ORDER BY total_plays DESC
LIMIT 5;


-- ============================================================
-- QUERY 08: Monthly revenue from successful payments
-- Business use: Core finance KPI — Monthly Recurring Revenue (MRR)
-- ============================================================
-- CONCEPT: WHERE filters rows before grouping
--          We only want status = 'Success' — no failed or refunded

SELECT
    DATE_FORMAT(payment_date, '%Y-%m')      AS payment_month,
    COUNT(*)                                AS transactions,
    ROUND(SUM(amount), 2)                   AS total_revenue,
    ROUND(AVG(amount), 2)                   AS avg_transaction,
    SUM(SUM(amount)) OVER (
        ORDER BY DATE_FORMAT(payment_date, '%Y-%m')
    )                                       AS cumulative_revenue
FROM payments
WHERE status = 'Success'
  AND payment_date IS NOT NULL
GROUP BY payment_month
ORDER BY payment_month;


-- ============================================================
-- QUERY 09: Payment success vs failure rate by payment method
-- Business use: Identify unreliable payment methods to fix/remove
-- ============================================================
-- CONCEPT: Multiple CASE WHEN in the same SELECT
--          We count successes and failures separately in one query

SELECT
    payment_method,
    COUNT(*)                                AS total_attempts,
    SUM(CASE WHEN status = 'Success'
             THEN 1 ELSE 0 END)            AS successful,
    SUM(CASE WHEN status = 'Failed'
             THEN 1 ELSE 0 END)            AS failed,
    ROUND(
        SUM(CASE WHEN status = 'Success'
                 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                       AS success_rate_pct
FROM payments
WHERE payment_method IS NOT NULL
  AND payment_method != 'Unknown'
GROUP BY payment_method
ORDER BY success_rate_pct DESC;


-- ============================================================
-- QUERY 10: Which users have the highest lifetime value?
--           (Top 20 users by total amount paid)
-- Business use: VIP user identification for loyalty programmes
-- ============================================================
-- CONCEPT: JOIN + GROUP BY + ORDER BY together
--          We join users and payments, sum up each user's spend

SELECT
    u.user_id,
    u.full_name,
    u.country,
    u.user_segment,
    COUNT(p.payment_id)                     AS total_payments,
    ROUND(SUM(p.amount), 2)                 AS lifetime_value,
    MIN(p.payment_date)                     AS first_payment,
    MAX(p.payment_date)                     AS latest_payment
FROM users u
JOIN payments p ON u.user_id = p.user_id
WHERE p.status = 'Success'
GROUP BY u.user_id, u.full_name, u.country, u.user_segment
ORDER BY lifetime_value DESC
LIMIT 20;


-- ============================================================
-- QUERY 11: Free-to-paid conversion analysis
--           What % of users who started Free upgraded to paid?
-- Business use: Funnel optimisation — freemium conversion rate
-- ============================================================
-- CONCEPT: Subquery — a query inside another query
--          The inner query identifies users who upgraded
--          The outer query counts them

SELECT
    COUNT(DISTINCT user_id)                 AS total_users,
    SUM(CASE WHEN previous_plan = 'Free'
             AND plan_type != 'Free'
             THEN 1 ELSE 0 END)            AS upgraded_users,
    ROUND(
        SUM(CASE WHEN previous_plan = 'Free'
                 AND plan_type != 'Free'
                 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(DISTINCT user_id), 2
    )                                       AS upgrade_rate_pct
FROM subscriptions
WHERE plan_type IS NOT NULL;


-- ============================================================
-- QUERY 12: Most common support issues and their resolution speed
-- Business use: Customer success team prioritisation
-- ============================================================
SELECT
    issue_type,
    COUNT(*)                                AS total_tickets,
    SUM(CASE WHEN resolved_date IS NOT NULL
             THEN 1 ELSE 0 END)            AS resolved,
    ROUND(
        SUM(CASE WHEN resolved_date IS NOT NULL
                 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                       AS resolution_rate_pct,
    ROUND(AVG(resolution_days), 1)          AS avg_days_to_resolve,
    ROUND(AVG(satisfaction_score), 2)       AS avg_csat
FROM support_tickets
WHERE issue_type != 'Unknown'
  AND issue_type IS NOT NULL
GROUP BY issue_type
ORDER BY total_tickets DESC;


-- ============================================================
-- QUERY 13: User activity summary per segment
--           How many plays, payments, and tickets per segment?
-- Business use: Understand which segments drive value vs cost
-- ============================================================
-- CONCEPT: Multiple JOINs — joining 3+ tables together
--          Each LEFT JOIN adds data from another table

SELECT
    u.user_segment,
    COUNT(DISTINCT u.user_id)               AS total_users,
    COUNT(DISTINCT le.event_id)             AS total_plays,
    ROUND(COUNT(DISTINCT le.event_id) * 1.0
          / COUNT(DISTINCT u.user_id), 1)   AS plays_per_user,
    COUNT(DISTINCT p.payment_id)            AS total_payments,
    ROUND(SUM(p.amount), 2)                 AS total_revenue,
    COUNT(DISTINCT t.ticket_id)             AS total_tickets
FROM users u
LEFT JOIN listening_events  le ON u.user_id = le.user_id
LEFT JOIN payments           p ON u.user_id = p.user_id
                                  AND p.status = 'Success'
LEFT JOIN support_tickets    t ON u.user_id = t.user_id
GROUP BY u.user_segment
ORDER BY total_revenue DESC;


-- ============================================================
-- QUERY 14: Churned users — last activity before churning
--           When did churned users stop engaging?
-- Business use: Define re-engagement campaign window
-- ============================================================
-- CONCEPT: DATEDIFF calculates the number of days between two dates
--          Subquery gets each churned user's last play date

SELECT
    u.user_id,
    u.country,
    s.plan_type                             AS last_plan,
    s.end_date                              AS churn_date,
    MAX(le.event_date)                      AS last_play_date,
    DATEDIFF(s.end_date, MAX(le.event_date))
                                            AS days_silent_before_churn
FROM users u
JOIN subscriptions   s  ON u.user_id = s.user_id
LEFT JOIN listening_events le ON u.user_id = le.user_id
WHERE s.status = 'Cancelled'
  AND s.end_date IS NOT NULL
  AND u.user_segment = 'Churned User'
GROUP BY u.user_id, u.country, s.plan_type, s.end_date
ORDER BY days_silent_before_churn DESC
LIMIT 20;


-- ============================================================
-- QUERY 15: Revenue at risk from failed payments
--           Which users have recent failed payments?
-- Business use: Billing recovery — reach out before they churn
-- ============================================================
-- CONCEPT: HAVING filters AFTER grouping (WHERE filters before)
--          We want users with MORE THAN 1 failed payment recently

SELECT
    u.user_id,
    u.full_name,
    u.email,
    u.country,
    s.plan_type,
    s.monthly_price,
    COUNT(p.payment_id)                     AS failed_attempts,
    MAX(p.payment_date)                     AS last_failed_date,
    DATEDIFF(CURDATE(), MAX(p.payment_date))
                                            AS days_since_last_failure
FROM users u
JOIN subscriptions  s ON u.user_id = s.user_id
JOIN payments       p ON u.user_id = p.user_id
WHERE p.status    = 'Failed'
  AND s.status    = 'Active'
  AND s.plan_type != 'Free'
GROUP BY u.user_id, u.full_name, u.email,
         u.country, s.plan_type, s.monthly_price
HAVING failed_attempts > 1
ORDER BY failed_attempts DESC, s.monthly_price DESC
LIMIT 25;


-- ============================================================
-- BONUS QUERY 16: Genre popularity by country (top genre per country)
-- Business use: Regional content licensing decisions
-- ============================================================
-- CONCEPT: CTE (Common Table Expression) with WITH
--          CTEs are like temporary tables — they make complex
--          queries much easier to read and understand
--          ROW_NUMBER() assigns a rank within each group

WITH genre_by_country AS (
    SELECT
        u.country,
        le.genre,
        COUNT(*)                            AS plays,
        ROW_NUMBER() OVER (
            PARTITION BY u.country
            ORDER BY COUNT(*) DESC
        )                                   AS genre_rank
    FROM listening_events le
    JOIN users u ON le.user_id = u.user_id
    WHERE le.genre  != 'Unknown'
      AND le.genre  IS NOT NULL
      AND u.country IS NOT NULL
    GROUP BY u.country, le.genre
)
SELECT
    country,
    genre                                   AS top_genre,
    plays
FROM genre_by_country
WHERE genre_rank = 1
ORDER BY plays DESC
LIMIT 20;


-- ============================================================
-- BONUS QUERY 17: Cohort retention — do users from 2022 still active?
-- Business use: Long-term retention analysis by signup year
-- ============================================================
SELECT
    YEAR(u.signup_date)                     AS signup_year,
    COUNT(DISTINCT u.user_id)               AS cohort_size,
    SUM(CASE WHEN s.status = 'Active'
             THEN 1 ELSE 0 END)            AS still_active,
    ROUND(
        SUM(CASE WHEN s.status = 'Active'
                 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(DISTINCT u.user_id), 2
    )                                       AS retention_rate_pct
FROM users u
LEFT JOIN subscriptions s ON u.user_id = s.user_id
WHERE u.signup_date IS NOT NULL
GROUP BY signup_year
ORDER BY signup_year;
