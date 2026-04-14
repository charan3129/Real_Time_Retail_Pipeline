{{ config(materialized='table') }}
SELECT {{ generate_surrogate_key(['product_id']) }} AS product_key,
    product_id, product_name, category, current_unit_price,
    CURRENT_DATE() AS effective_from, NULL AS effective_to, TRUE AS is_current
FROM {{ ref('stg_products') }}
