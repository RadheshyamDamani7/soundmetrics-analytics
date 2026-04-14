-- ============================================================
--  SoundMetrics — Phase 4: SQL Analysis
--  File        : 01_create_schema.sql
--  Description : Create the SoundMetrics database and all 5 tables
--  Run this    : Paste into MySQL Workbench and click ⚡ Execute
-- ============================================================

-- Step 1: Create the database
-- IF NOT EXISTS means it won't throw an error if it already exists
CREATE DATABASE IF NOT EXISTS soundmetrics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Step 2: Tell MySQL to use this database for all queries below
USE soundmetrics;

-- ============================================================
-- TABLE 1: users
-- ============================================================
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id             INT             NOT NULL,
    full_name           VARCHAR(150),
    email               VARCHAR(200),
    age                 INT,
    gender              VARCHAR(30),
    country             VARCHAR(100),
    currency            VARCHAR(10),
    signup_date         DATE,
    acquisition_channel VARCHAR(100),
    user_segment        VARCHAR(50),
    PRIMARY KEY (user_id)
);

-- ============================================================
-- TABLE 2: subscriptions
-- ============================================================
DROP TABLE IF EXISTS subscriptions;

CREATE TABLE subscriptions (
    sub_id          INT             NOT NULL,
    user_id         INT,
    plan_type       VARCHAR(50),
    previous_plan   VARCHAR(50),
    status          VARCHAR(50),
    start_date      DATE,
    end_date        DATE,
    monthly_price   DECIMAL(10, 2),
    currency        VARCHAR(10),
    auto_renew      TINYINT(1),
    PRIMARY KEY (sub_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON DELETE CASCADE
);

-- ============================================================
-- TABLE 3: listening_events
-- ============================================================
DROP TABLE IF EXISTS listening_events;

CREATE TABLE listening_events (
    event_id            BIGINT          NOT NULL,
    user_id             INT,
    song_id             INT,
    genre               VARCHAR(100),
    listen_duration_sec INT,
    device              VARCHAR(50),
    event_date          DATE,
    event_time          TIME,
    is_skip             TINYINT(1),
    is_repeat_listen    TINYINT(1),
    PRIMARY KEY (event_id),
    INDEX idx_user_id   (user_id),
    INDEX idx_event_date(event_date),
    INDEX idx_genre     (genre)
);

-- ============================================================
-- TABLE 4: payments
-- ============================================================
DROP TABLE IF EXISTS payments;

CREATE TABLE payments (
    payment_id      INT             NOT NULL,
    user_id         INT,
    amount          DECIMAL(12, 2),
    currency        VARCHAR(10),
    payment_date    DATE,
    payment_method  VARCHAR(50),
    status          VARCHAR(30),
    promo_code      VARCHAR(30),
    refund_reason   VARCHAR(100),
    PRIMARY KEY (payment_id),
    INDEX idx_pay_user (user_id),
    INDEX idx_pay_date (payment_date)
);

-- ============================================================
-- TABLE 5: support_tickets
-- ============================================================
DROP TABLE IF EXISTS support_tickets;

CREATE TABLE support_tickets (
    ticket_id           INT             NOT NULL,
    user_id             INT,
    issue_type          VARCHAR(100),
    support_channel     VARCHAR(50),
    created_date        DATE,
    resolved_date       DATE,
    resolution_days     DECIMAL(5,1),
    satisfaction_score  TINYINT,
    is_repeat_contact   TINYINT(1),
    PRIMARY KEY (ticket_id),
    INDEX idx_ticket_user (user_id)
);

-- ============================================================
-- Verify all tables were created
-- ============================================================
SHOW TABLES;
SELECT
    table_name                              AS 'Table',
    table_rows                              AS 'Estimated Rows',
    ROUND(data_length / 1024 / 1024, 2)    AS 'Data Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'soundmetrics'
ORDER BY table_name;
