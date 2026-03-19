{{ 
    config(
        schema='GOLD',       
        materialized='view'  
    ) 
}}
WITH sales AS (
    SELECT
        oi.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.CATEGORY,
        SUM(oi.QTY) AS TOTAL_QTY_SOLD,
        SUM(oi.TOTAL_PRICE) AS TOTAL_SALES,
        SUM(oi.TOTAL_PRICE - (p.COST_AMT * oi.QTY)) AS TOTAL_MARGIN
    FROM {{ ref('order_items_enriched') }} oi
    JOIN {{ ref('products_enriched') }} p
        ON oi.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY 1,2,3
)

SELECT
    *,
    RANK() OVER (PARTITION BY CATEGORY ORDER BY TOTAL_SALES DESC) AS CATEGORY_RANK,
    RANK() OVER (ORDER BY TOTAL_SALES DESC) AS GLOBAL_RANK
FROM sales
