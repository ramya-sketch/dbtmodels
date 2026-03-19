{{ 
    config(
        schema='SILVER',       
        materialized='table'  
    ) 
}}
WITH sales_summary AS (
    SELECT
        PRODUCT_ID,
        SUM(QTY) AS total_qty_sold,
        SUM(TOTAL_PRICE) AS total_sales
    FROM {{ source('dqlabs_qa', 'ORDER_ITEMS') }}
    GROUP BY PRODUCT_ID
)

SELECT
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUB_CATEGORY,
    p.BRAND,
    p.PRICE_AMT,
    p.COST_AMT,
    p.MARGIN_PCT,
    p.STOCK_QTY,
    ss.total_qty_sold,
    ss.total_sales
FROM {{ source('dqlabs_qa', 'PRODUCTS') }} p
LEFT JOIN sales_summary ss
    ON p.PRODUCT_ID = ss.PRODUCT_ID


