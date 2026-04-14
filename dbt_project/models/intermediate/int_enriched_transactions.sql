{{ config(materialized='ephemeral') }}
SELECT t.*, p.current_unit_price AS catalog_unit_price,
    CASE WHEN t.unit_price != p.current_unit_price THEN TRUE ELSE FALSE END AS has_price_discrepancy
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id
