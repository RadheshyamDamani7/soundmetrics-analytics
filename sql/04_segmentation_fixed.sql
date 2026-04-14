-- ============================================================
--  SoundMetrics — Phase 4: SQL Analysis
--  File        : 04_segmentation.sql (Fixed for MySQL 8.0)
-- ============================================================

USE soundmetrics;

-- ============================================================
-- SEGMENT QUERY 01: Full RFM Scoring
-- ============================================================

WITH rfm_raw AS (
    SELECT
        u.user_id,
        u.full_name,
        u.country,
        u.user_segment,
        s.plan_type,
        DATEDIFF(CURDATE(), MAX(le.event_date))  AS recency_days,
        COUNT(DISTINCT le.event_id)              AS frequency,
        COALESCE(SUM(p.amount), 0)               AS monetary
    FROM users u
    LEFT JOIN listening_events le ON u.user_id = le.user_id
    LEFT JOIN payments          p ON u.user_id = p.user_id
                                     AND p.status = 'Success'
    LEFT JOIN subscriptions     s ON u.user_id = s.user_id
    GROUP BY u.user_id, u.full_name, u.country, u.user_segment, s.plan_type
),
rfm_scored AS (
    SELECT
        user_id, full_name, country, user_segment, plan_type,
        recency_days, frequency, monetary,
        (6 - NTILE(5) OVER (ORDER BY recency_days DESC)) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)           AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)            AS m_score
    FROM rfm_raw
    WHERE recency_days IS NOT NULL
)
SELECT
    user_id,
    full_name,
    country,
    user_segment,
    plan_type,
    recency_days,
    frequency,
    ROUND(monetary, 2)              AS monetary,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)   AS rfm_total,
    CASE
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champions'
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal Users'
        WHEN r_score >= 4 AND (f_score + m_score) < 6 THEN 'Promising'
        WHEN r_score <= 2 AND (f_score + m_score) >= 8 THEN 'At Risk'
        WHEN (r_score + f_score + m_score) <= 5 THEN 'Lost Users'
        ELSE 'Regular'
    END                             AS rfm_segment
FROM rfm_scored
ORDER BY rfm_total DESC;


-- ============================================================
-- SEGMENT QUERY 02: RFM Segment Summary
-- ============================================================

WITH rfm_raw AS (
    SELECT
        u.user_id,
        DATEDIFF(CURDATE(), MAX(le.event_date))  AS recency_days,
        COUNT(DISTINCT le.event_id)              AS frequency,
        COALESCE(SUM(p.amount), 0)               AS monetary
    FROM users u
    LEFT JOIN listening_events le ON u.user_id = le.user_id
    LEFT JOIN payments          p ON u.user_id = p.user_id
                                     AND p.status = 'Success'
    GROUP BY u.user_id
    HAVING recency_days IS NOT NULL
),
rfm_scored AS (
    SELECT *,
        (6 - NTILE(5) OVER (ORDER BY recency_days DESC)) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)           AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)            AS m_score
    FROM rfm_raw
)
SELECT
    CASE
        WHEN (r_score+f_score+m_score) >= 13 THEN 'Champions'
        WHEN (r_score+f_score+m_score) >= 10 THEN 'Loyal Users'
        WHEN r_score >= 4 AND (f_score+m_score) < 6 THEN 'Promising'
        WHEN r_score <= 2 AND (f_score+m_score) >= 8 THEN 'At Risk'
        WHEN (r_score+f_score+m_score) <= 5 THEN 'Lost Users'
        ELSE 'Regular'
    END                             AS rfm_segment,
    COUNT(*)                        AS user_count,
    ROUND(AVG(recency_days), 1)     AS avg_recency_days,
    ROUND(AVG(frequency), 1)        AS avg_plays,
    ROUND(AVG(monetary), 2)         AS avg_revenue,
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM rfm_raw), 2) AS pct_of_users
FROM rfm_scored
GROUP BY rfm_segment
ORDER BY avg_revenue DESC;


-- ============================================================
-- SEGMENT QUERY 03: Dormant user recovery opportunities
-- ============================================================

SELECT
    u.user_id,
    u.full_name,
    u.country,
    s.plan_type                              AS last_plan,
    s.monthly_price,
    COUNT(le.event_id)                       AS lifetime_plays,
    MAX(le.event_date)                       AS last_active_date,
    DATEDIFF(CURDATE(), MAX(le.event_date))  AS days_inactive,
    COALESCE(SUM(p.amount), 0)               AS lifetime_revenue,
    ROUND(
        (COUNT(le.event_id) * 0.4)
        + (COALESCE(SUM(p.amount), 0) * 0.6)
    , 2)                                     AS recovery_score
FROM users u
JOIN subscriptions      s  ON u.user_id = s.user_id
LEFT JOIN listening_events le ON u.user_id = le.user_id
LEFT JOIN payments          p ON u.user_id = p.user_id
                                 AND p.status = 'Success'
WHERE u.user_segment IN ('Dormant User', 'Churned User')
  AND s.plan_type != 'Free'
GROUP BY u.user_id, u.full_name, u.country, s.plan_type, s.monthly_price
HAVING days_inactive IS NOT NULL
ORDER BY recovery_score DESC
LIMIT 30;


-- ============================================================
-- SEGMENT QUERY 04: Behaviour profile per user segment
-- ============================================================

SELECT
    u.user_segment,
    COUNT(DISTINCT u.user_id)            AS users,
    ROUND(AVG(le_stats.plays), 1)        AS avg_lifetime_plays,
    ROUND(AVG(le_stats.avg_dur), 1)      AS avg_session_duration_sec,
    ROUND(AVG(le_stats.skip_pct), 2)     AS avg_skip_rate_pct,
    ROUND(AVG(le_stats.unique_genres),1) AS avg_genres_explored,
    ROUND(AVG(pay_stats.total_paid), 2)  AS avg_lifetime_revenue,
    ROUND(AVG(t_stats.tickets), 2)       AS avg_support_tickets
FROM users u
LEFT JOIN (
    SELECT
        user_id,
        COUNT(*)                 AS plays,
        AVG(listen_duration_sec) AS avg_dur,
        AVG(is_skip) * 100       AS skip_pct,
        COUNT(DISTINCT genre)    AS unique_genres
    FROM listening_events
    GROUP BY user_id
) le_stats ON u.user_id = le_stats.user_id
LEFT JOIN (
    SELECT user_id, COALESCE(SUM(amount), 0) AS total_paid
    FROM payments
    WHERE status = 'Success'
    GROUP BY user_id
) pay_stats ON u.user_id = pay_stats.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS tickets
    FROM support_tickets
    GROUP BY user_id
) t_stats ON u.user_id = t_stats.user_id
GROUP BY u.user_segment
ORDER BY avg_lifetime_revenue DESC;


-- ============================================================
-- SEGMENT QUERY 05: Master export table for Power BI
-- Run this last — export result as user_master_table.csv
-- Results Grid toolbar → Export → save to data/exports/
-- ============================================================

SELECT
    u.user_id,
    u.full_name,
    u.country,
    u.age,
    u.gender,
    u.currency,
    u.signup_date,
    u.acquisition_channel,
    u.user_segment,
    s.plan_type,
    s.status                                    AS sub_status,
    s.monthly_price,
    s.auto_renew,
    COALESCE(le.total_plays, 0)                 AS total_plays,
    COALESCE(le.avg_duration, 0)                AS avg_listen_duration,
    COALESCE(le.skip_rate, 0)                   AS skip_rate_pct,
    COALESCE(le.unique_genres, 0)               AS unique_genres,
    COALESCE(le.days_since_last_play, 999)       AS days_since_last_play,
    COALESCE(pay.total_revenue, 0)              AS lifetime_revenue,
    COALESCE(pay.payment_count, 0)              AS payment_count,
    COALESCE(pay.failed_payments, 0)            AS failed_payment_count,
    COALESCE(t.ticket_count, 0)                 AS support_tickets,
    COALESCE(t.avg_csat, 0)                     AS avg_csat_score
FROM users u
LEFT JOIN subscriptions s ON u.user_id = s.user_id
LEFT JOIN (
    SELECT
        user_id,
        COUNT(*)                             AS total_plays,
        ROUND(AVG(listen_duration_sec), 1)   AS avg_duration,
        ROUND(AVG(is_skip) * 100, 2)         AS skip_rate,
        COUNT(DISTINCT genre)                AS unique_genres,
        DATEDIFF(CURDATE(), MAX(event_date)) AS days_since_last_play
    FROM listening_events
    GROUP BY user_id
) le ON u.user_id = le.user_id
LEFT JOIN (
    SELECT
        user_id,
        ROUND(SUM(CASE WHEN status='Success' THEN amount ELSE 0 END), 2) AS total_revenue,
        SUM(CASE WHEN status='Success' THEN 1 ELSE 0 END)                AS payment_count,
        SUM(CASE WHEN status='Failed'  THEN 1 ELSE 0 END)                AS failed_payments
    FROM payments
    GROUP BY user_id
) pay ON u.user_id = pay.user_id
LEFT JOIN (
    SELECT
        user_id,
        COUNT(*)                           AS ticket_count,
        ROUND(AVG(satisfaction_score), 2)  AS avg_csat
    FROM support_tickets
    GROUP BY user_id
) t ON u.user_id = t.user_id
ORDER BY u.user_id
LIMIT 100000;
