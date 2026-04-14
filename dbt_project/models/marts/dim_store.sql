{{ config(materialized='table') }}
SELECT {{ generate_surrogate_key(['store_id']) }} AS store_key,
    store_id, store_name, region, state, city
FROM {{ ref('stg_stores') }}
