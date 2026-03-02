{{ 
    config(
        schema='GOLD',       
        materialized='view'  
    ) 
}}
WITH orders AS (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.EMPLOYEE_ID,
        o.ORDER_TOTAL,
        o.DISCOUNT_AMT,
        o.TAX_AMT,
        o.SHIPPING_AMT,
        COUNT(oi.ORDER_ITEM_ID) AS NUM_ITEMS,
        SUM(oi.TOTAL_PRICE) AS ITEMS_TOTAL
    FROM {{ ref('orders_enriched') }} o
    LEFT JOIN {{ ref('order_items_enriched') }} oi
        ON o.ORDER_ID = oi.ORDER_ID
    GROUP BY o.ORDER_ID, o.CUSTOMER_ID, o.EMPLOYEE_ID, o.ORDER_TOTAL, o.DISCOUNT_AMT, o.TAX_AMT, o.SHIPPING_AMT
)

SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
    o.EMPLOYEE_ID,
    e.FIRST_NAME || ' ' || e.LAST_NAME AS EMPLOYEE_NAME,
    o.ORDER_TOTAL,
    o.DISCOUNT_AMT,
    o.TAX_AMT,
    o.SHIPPING_AMT,
    o.NUM_ITEMS,
    o.ITEMS_TOTAL,
    o.ORDER_TOTAL - o.DISCOUNT_AMT + o.TAX_AMT + o.SHIPPING_AMT AS NET_REVENUE
FROM orders o
LEFT JOIN {{ ref('customer_enriched') }} c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('employee_enriched') }} e
    ON o.EMPLOYEE_ID = e.EMPLOYEE_ID
