{{ 
    config(
        schema='SILVER',       
        materialized='table'  
    ) 
}}

WITH employee_orders AS (
    SELECT
        EMPLOYEE_ID,
        COUNT(ORDER_ID) AS num_orders_handled,
        SUM(ORDER_TOTAL) AS total_sales
    FROM {{ source('dqlabs_qa', 'ORDERS') }}
    GROUP BY EMPLOYEE_ID
)

SELECT
    e.EMPLOYEE_ID,
    e.FIRST_NAME,
    e.LAST_NAME,
    e.DEPARTMENT,
    e.ROLE,
    eo.num_orders_handled,
    eo.total_sales
FROM {{ source('dqlabs_qa', 'EMPLOYEE') }} e
LEFT JOIN employee_orders eo
    ON e.EMPLOYEE_ID = eo.EMPLOYEE_ID

