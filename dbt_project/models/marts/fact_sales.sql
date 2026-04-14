{{ config(materialized='table', cluster_by=['date_key','store_key']) }}
SELECT
    {{ generate_surrogate_key(['order_id','transaction_timestamp']) }} AS sale_key,
    order_id,
    {{ generate_surrogate_key(['product_id']) }} AS product_key,
    {{ generate_surrogate_key(['store_id']) }} AS store_key,
    CAST(TO_CHAR(transaction_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    quantity, unit_price, total_amount, payment_method, customer_segment, transaction_timestamp
FROM {{ ref('int_enriched_transactions') }}
