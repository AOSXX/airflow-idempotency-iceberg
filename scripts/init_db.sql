-- ============================================================
-- Runs once on first postgres container start as superuser
-- Creates Airflow metadata DB + source/target demo databases
-- ============================================================

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE source_db OWNER airflow;

-- ── Source database ──────────────────────────────────────────
\connect source_db airflow

CREATE TABLE orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INT            NOT NULL,
    product     VARCHAR(100)   NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    order_date  DATE           NOT NULL,
    status      VARCHAR(20)    NOT NULL DEFAULT 'completed',
    created_at  TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- Seed 200 rows spread across the last 10 days
INSERT INTO orders (customer_id, product, amount, order_date, status)
SELECT
    (random() * 99 + 1)::INT,
    (ARRAY['Widget A','Widget B','Gadget X','Gadget Y','Gizmo Z'])[floor(random() * 5 + 1)::INT],
    round((random() * 990 + 10)::NUMERIC, 2),
    CURRENT_DATE - (n % 10),
    (ARRAY['completed','pending','cancelled'])[floor(random() * 3 + 1)::INT]
FROM generate_series(1, 200) AS n;

-- ── Target tables (SQL idempotency demos) ────────────────────

-- BAD target: no constraint, duplicates will accumulate
CREATE TABLE orders_raw (
    order_id    INT,
    customer_id INT,
    product     VARCHAR(100),
    amount      NUMERIC(10, 2),
    order_date  DATE,
    status      VARCHAR(20),
    loaded_at   TIMESTAMP DEFAULT NOW()
);

-- GOOD target: primary key on (summary_date) prevents duplicates
CREATE TABLE orders_daily_summary (
    summary_date     DATE           NOT NULL PRIMARY KEY,
    total_orders     INT            NOT NULL,
    total_revenue    NUMERIC(10, 2) NOT NULL,
    completed_orders INT            NOT NULL,
    avg_order_value  NUMERIC(10, 2) NOT NULL,
    refreshed_at     TIMESTAMP      NOT NULL DEFAULT NOW()
);
