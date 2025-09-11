{{ config(
    materialized = 'table',
    tags = ['gold', 'electronics']
) }}

WITH fct_sales AS (
    SELECT * 
    FROM {{ ref('fct_electronics_sales') }}
)

SELECT 
    RECORD_ID,
    SALE_DATE,
    STORE_ID,
    PRODUCT_NAME,
    SALE_YEAR,
    CURRENT_TIMESTAMP() AS LOADED_AT,
    '{{ invocation_id }}' AS DBT_RUN_ID
FROM fct_saless
WHERE SALE_YEAR IS NOT NULL 
