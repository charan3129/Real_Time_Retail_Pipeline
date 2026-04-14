{{ config(materialized='table') }}
WITH date_spine AS ({{ dbt_utils.date_spine(datepart="day", start_date="cast('2020-01-01' as date)", end_date="cast('2030-12-31' as date)") }})
SELECT CAST(TO_CHAR(date_day,'YYYYMMDD') AS INTEGER) AS date_key, date_day AS full_date,
    YEAR(date_day) AS year, QUARTER(date_day) AS quarter, MONTH(date_day) AS month,
    MONTHNAME(date_day) AS month_name, WEEKOFYEAR(date_day) AS week_of_year,
    DAYOFMONTH(date_day) AS day_of_month, DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
