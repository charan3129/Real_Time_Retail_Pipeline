USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA ANALYTICS;

-- 1. Revenue trend with 7-day and 30-day moving averages
SELECT full_date, total_revenue,
    AVG(total_revenue) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rev_7day_ma,
    AVG(total_revenue) OVER (ORDER BY full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rev_30day_ma
FROM ANALYTICS.V_DAILY_REVENUE ORDER BY full_date DESC LIMIT 90;

-- 2. Top 10 products by revenue within each category
SELECT product_name, category, total_revenue, total_units,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_rank
FROM ANALYTICS.V_TOP_PRODUCTS QUALIFY category_rank <= 10
ORDER BY category, category_rank;

-- 3. Store performance with percentile ranking
SELECT store_name, region, state, total_revenue, total_orders,
    NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile,
    PERCENT_RANK() OVER (ORDER BY total_revenue) AS revenue_percentile
FROM ANALYTICS.V_STORE_PERFORMANCE ORDER BY total_revenue DESC;

-- 4. Month-over-month growth by region
WITH monthly AS (
    SELECT region, year, month, total_revenue,
        LAG(total_revenue) OVER (PARTITION BY region ORDER BY year, month) AS prev_month
    FROM ANALYTICS.V_REVENUE_BY_REGION_MONTH
)
SELECT region, year, month, total_revenue, prev_month,
    CASE WHEN prev_month > 0
        THEN ROUND((total_revenue - prev_month) / prev_month * 100, 2)
        ELSE NULL END AS mom_growth_pct
FROM monthly ORDER BY region, year, month;

-- 5. Customer segment cohort analysis
SELECT f.customer_segment, d.year, d.quarter,
    COUNT(DISTINCT f.order_id) AS orders, SUM(f.total_amount) AS revenue,
    AVG(f.total_amount) AS avg_order_value
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
GROUP BY f.customer_segment, d.year, d.quarter
ORDER BY f.customer_segment, d.year, d.quarter;

-- 6. Anomaly detection: daily count vs 30-day rolling average
WITH daily AS (
    SELECT d.full_date, COUNT(*) AS txn_count
    FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
    GROUP BY d.full_date
), stats AS (
    SELECT full_date, txn_count,
        AVG(txn_count) OVER (ORDER BY full_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS avg_30d,
        STDDEV(txn_count) OVER (ORDER BY full_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS std_30d
    FROM daily
)
SELECT full_date, txn_count, ROUND(avg_30d,2) AS avg_30d, ROUND(std_30d,2) AS std_30d,
    CASE WHEN std_30d > 0 AND ABS(txn_count - avg_30d) > 2 * std_30d THEN 'ANOMALY' ELSE 'NORMAL' END AS flag,
    CASE WHEN std_30d > 0 THEN ROUND((txn_count - avg_30d)/std_30d, 2) ELSE 0 END AS z_score
FROM stats WHERE std_30d IS NOT NULL ORDER BY full_date DESC;

-- 7. Hourly sales heatmap
SELECT d.day_name, EXTRACT(HOUR FROM f.transaction_timestamp) AS hour_of_day,
    COUNT(*) AS txn_count, SUM(f.total_amount) AS revenue
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.day_name, EXTRACT(HOUR FROM f.transaction_timestamp)
ORDER BY CASE d.day_name WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3 WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6 WHEN 'Sunday' THEN 7 END, hour_of_day;
