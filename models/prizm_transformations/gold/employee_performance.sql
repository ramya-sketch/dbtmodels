{{ 
    config(
        schema='GOLD',       
        materialized='view'  
    ) 
}}
WITH emp_orders AS (
    SELECT
        e.EMPLOYEE_ID,
        e.FIRST_NAME,
        e.LAST_NAME,
        e.DEPARTMENT,
        e.ROLE,
        o.ORDER_ID,
        o.ORDER_TOTAL
    FROM {{ ref('employee_enriched') }} e
    LEFT JOIN {{ ref('orders_enriched') }} o
        ON e.EMPLOYEE_ID = o.EMPLOYEE_ID
)

SELECT
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    DEPARTMENT,
    ROLE,
    COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS_HANDLED,
    SUM(ORDER_TOTAL) AS TOTAL_REVENUE_HANDLED,
    AVG(ORDER_TOTAL) AS AVG_ORDER_VALUE,
    CASE WHEN SUM(ORDER_TOTAL) > 50000 THEN 0.05*SUM(ORDER_TOTAL)
         ELSE 0.02*SUM(ORDER_TOTAL) END AS ESTIMATED_COMMISSION
FROM emp_orders
GROUP BY EMPLOYEE_ID, FIRST_NAME, LAST_NAME, DEPARTMENT, ROLE
