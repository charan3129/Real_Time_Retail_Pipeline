{{ config(materialized='view') }}
WITH ranked AS (
    SELECT product_id, product_name, category, unit_price AS current_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY transaction_timestamp DESC) AS rn
    FROM {{ ref('stg_transactions') }}
)
SELECT product_id, product_name, category, current_unit_price FROM ranked WHERE rn = 1
