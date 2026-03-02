{{ 
    config(
        schema='GOLD',       
        materialized='view'  
    ) 
}}
WITH customer_orders AS (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.ORDER_TS,
        o.ORDER_TOTAL,
        o.DISCOUNT_AMT,
        o.TAX_AMT
    FROM {{ ref('orders_enriched') }} o
),

order_items AS (
    SELECT
        oi.ORDER_ID,
        oi.PRODUCT_ID,
        oi.QTY,
        oi.TOTAL_PRICE
    FROM {{ ref('order_items_enriched') }} oi
),

products AS (
    SELECT
        PRODUCT_ID,
        CATEGORY
    FROM {{ ref('products_enriched') }}
)

SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM(o.ORDER_TOTAL) AS TOTAL_SPEND,
    AVG(o.ORDER_TOTAL) AS AVG_ORDER_VALUE,
    MAX(o.ORDER_TS) AS LAST_ORDER_DATE,
    COUNT(DISTINCT CASE WHEN o.ORDER_TS >= DATEADD(year, -1, CURRENT_DATE()) THEN o.ORDER_ID END) AS ORDERS_LAST_YEAR,
    MAX(p.CATEGORY) KEEP (DENSE_RANK FIRST ORDER BY SUM(oi.TOTAL_PRICE) DESC) AS FAVORITE_CATEGORY
FROM {{ ref('customer_enriched') }} c
LEFT JOIN customer_orders o ON c.CUSTOMER_ID = o.CUSTOMER_ID
LEFT JOIN order_items oi ON o.ORDER_ID = oi.ORDER_ID
LEFT JOIN products p ON oi.PRODUCT_ID = p.PRODUCT_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME
