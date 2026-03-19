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
),

-- Aggregate category spend per customer
customer_category_spend AS (
    SELECT
        c.CUSTOMER_ID,
        p.CATEGORY,
        SUM(oi.TOTAL_PRICE) AS TOTAL_SPENT
    FROM {{ ref('customer_enriched') }} c
    LEFT JOIN customer_orders o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    LEFT JOIN order_items oi ON o.ORDER_ID = oi.ORDER_ID
    LEFT JOIN products p ON oi.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY c.CUSTOMER_ID, p.CATEGORY
),

favorite_category AS (
    SELECT
        CUSTOMER_ID,
        CATEGORY AS FAVORITE_CATEGORY
    FROM (
        SELECT
            CUSTOMER_ID,
            CATEGORY,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY TOTAL_SPENT DESC) AS rn
        FROM customer_category_spend
    )
    WHERE rn = 1
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
    fc.FAVORITE_CATEGORY
FROM {{ ref('customer_enriched') }} c
LEFT JOIN customer_orders o ON c.CUSTOMER_ID = o.CUSTOMER_ID
LEFT JOIN favorite_category fc ON c.CUSTOMER_ID = fc.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, fc.FAVORITE_CATEGORY
