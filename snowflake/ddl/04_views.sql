USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW ANALYTICS.V_DAILY_REVENUE AS
SELECT d.full_date, d.year, d.month_name, d.day_name, d.is_weekend,
    COUNT(DISTINCT f.order_id) AS total_orders, SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue, AVG(f.total_amount) AS avg_order_value,
    COUNT(DISTINCT f.store_key) AS active_stores
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month_name, d.day_name, d.is_weekend
ORDER BY d.full_date DESC;

CREATE OR REPLACE VIEW ANALYTICS.V_TOP_PRODUCTS AS
SELECT p.product_id, p.product_name, p.category,
    SUM(f.total_amount) AS total_revenue, SUM(f.quantity) AS total_units,
    COUNT(DISTINCT f.order_id) AS total_orders,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS revenue_rank
FROM ANALYTICS.FACT_SALES f
JOIN ANALYTICS.DIM_PRODUCT p ON f.product_key = p.product_key AND p.is_current = TRUE
GROUP BY p.product_id, p.product_name, p.category;

CREATE OR REPLACE VIEW ANALYTICS.V_STORE_PERFORMANCE AS
SELECT s.store_id, s.store_name, s.region, s.state, s.city,
    COUNT(DISTINCT f.order_id) AS total_orders, SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_order_value, SUM(f.quantity) AS total_units_sold
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_STORE s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_name, s.region, s.state, s.city;

CREATE OR REPLACE VIEW ANALYTICS.V_REVENUE_BY_REGION_MONTH AS
SELECT s.region, d.year, d.month, d.month_name,
    SUM(f.total_amount) AS total_revenue, COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT s.store_id) AS active_stores
FROM ANALYTICS.FACT_SALES f
JOIN ANALYTICS.DIM_STORE s ON f.store_key = s.store_key
JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
GROUP BY s.region, d.year, d.month, d.month_name ORDER BY s.region, d.year, d.month;

CREATE OR REPLACE VIEW ANALYTICS.V_PAYMENT_ANALYSIS AS
SELECT f.payment_method, d.year, d.month,
    COUNT(DISTINCT f.order_id) AS order_count, SUM(f.total_amount) AS total_revenue,
    RATIO_TO_REPORT(SUM(f.total_amount)) OVER (PARTITION BY d.year, d.month) AS revenue_share
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
GROUP BY f.payment_method, d.year, d.month;

CREATE OR REPLACE VIEW ANALYTICS.V_RECENT_TRANSACTIONS AS
SELECT DATE_TRUNC('HOUR', f.transaction_timestamp) AS hour_bucket,
    COUNT(*) AS transaction_count, SUM(f.total_amount) AS hourly_revenue
FROM ANALYTICS.FACT_SALES f
WHERE f.transaction_timestamp >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', f.transaction_timestamp) ORDER BY hour_bucket DESC;
