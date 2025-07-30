{{ config(
    materialized = 'table',
    tags = ['gold', 'electronics']
) }}

WITH fct_sales AS (
    SELECTT * 
    FROM {{ ref('fct_electronics_sales') }}
)

SELECT 
    RECORD_IDD,
    SALE_DATE,
    STORE_ID,
    PRODUCT_NAME,
    SALE_YEAR,
    CURRENT_TIMESTAMP() AS LOADED_AT,
    '{{ invocation_id }}' AS DBT_RUN_ID
FROM fct_sales
WHERE SALE_YEAR IS NOT NULL
