{{ 
    config(
        schema='SILVER',       
        materialized='table'  
    ) 
}}

WITH order_items_summary AS (
    SELECT
        ORDER_ID,
        SUM(QTY) AS total_qty,
        SUM(TOTAL_PRICE) AS total_items_price
    FROM {{ source('dqlabs_qa', 'ORDER_ITEMS') }}
    GROUP BY ORDER_ID
)

SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.EMPLOYEE_ID,
    o.ORDER_TS,
    o.ORDER_STATUS,
    o.PAYMENT_TYPE,
    o.ORDER_TOTAL,
    o.DISCOUNT_AMT,
    o.TAX_AMT,
    o.SHIPPING_AMT,
    oi.total_qty,
    oi.total_items_price
FROM {{ source('dqlabs_qa', 'ORDERS') }} o
LEFT JOIN order_items_summary oi
    ON o.ORDER_ID = oi.ORDER_ID
