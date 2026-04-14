{{ config(materialized='view') }}
WITH source AS (SELECT * FROM {{ source('raw', 'transactions') }}),
cleaned AS (
    SELECT order_id, product_id,
        COALESCE(NULLIF(TRIM(product_name),''), 'Unknown Product') AS product_name,
        COALESCE(NULLIF(TRIM(category),''), 'Uncategorized') AS category,
        COALESCE(quantity, 1) AS quantity,
        COALESCE(unit_price, 0.0) AS unit_price,
        COALESCE(total_amount, quantity * unit_price) AS total_amount,
        store_id, COALESCE(NULLIF(TRIM(store_name),''), 'Unknown Store') AS store_name,
        region, state, city,
        LOWER(TRIM(COALESCE(payment_method, 'unknown'))) AS payment_method,
        LOWER(TRIM(COALESCE(customer_segment, 'unknown'))) AS customer_segment,
        transaction_timestamp, DATE(transaction_timestamp) AS transaction_date,
        event_type, ingestion_timestamp, batch_id, row_hash, _loaded_at
    FROM source WHERE order_id IS NOT NULL AND product_id IS NOT NULL AND store_id IS NOT NULL
)
SELECT * FROM cleaned
