{{ 
    config(
        schema='SILVER',       
        materialized='table'  
    ) 
}}

WITH orders_summary AS (
    SELECT
        CUSTOMER_ID,
        COUNT(ORDER_ID) AS total_orders,
        SUM(ORDER_TOTAL) AS total_spend,
        MAX(ORDER_TS) AS last_order_date
    FROM {{ source('dqlabs_qa', 'ORDERS') }}
    GROUP BY CUSTOMER_ID
)

SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL_ADDR,
    c.PHONE_REF,
    c.DOB_INFO,
    c.CUSTOMER_AGE,
    c.GENDER,
    c.COUNTRY,
    os.total_orders,
    os.total_spend,
    os.last_order_date
FROM {{ source('dqlabs_qa', 'CUSTOMER') }} c
LEFT JOIN orders_summary os
    ON c.CUSTOMER_ID = os.CUSTOMER_ID
