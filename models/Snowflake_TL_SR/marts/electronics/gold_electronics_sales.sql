{{
  config(
    materialized='table',
    tags=['gold', 'electronics']
  )
}}
WITH fct_sales AS (
    SELECT * 
    FROM DQLABS_QA.DBT_MODELS.fct_electronics_sales
)

SELECT 
    RECORD_ID
   ,DATE,
    STORE_ID,
    PRODUCT_NAME,
    SALE_YEAR
FROM fct_sales
WHERE SALE_YEAR IS NOT NULL