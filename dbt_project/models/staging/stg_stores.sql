{{ config(materialized='view') }}
WITH ranked AS (
    SELECT store_id, store_name, region, state, city,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY transaction_timestamp DESC) AS rn
    FROM {{ ref('stg_transactions') }}
)
SELECT store_id, store_name, region, state, city FROM ranked WHERE rn = 1
