SELECT sale_key, total_amount FROM {{ ref('fact_sales') }} WHERE total_amount < 0
