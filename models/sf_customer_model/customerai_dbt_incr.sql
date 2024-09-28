{{ 
config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='delete+insert'
)
}}

WITH customer_data AS (
    SELECT 
        o.ORDER_ID, 
        o.CUSTOMER_ID, 
        o.ORDER_DATE, 
        o.STATUS, 
        c.FIRST_NAME, 
        c.LAST_NAME,
        ROW_NUMBER() OVER (ORDER BY o.ORDER_DATE DESC) AS row_num
    FROM 
        {{ ref('stg_orders') }} o
    JOIN 
        {{ ref('stg_customer') }} c
    ON 
        o.CUSTOMER_ID = c.CUSTOMER_ID
)

SELECT 
    ORDER_ID, 
    CUSTOMER_ID, 
    ORDER_DATE, 
    STATUS, 
    FIRST_NAME, 
    LAST_NAME
FROM 
    customer_data

{% if is_incremental() %}
    -- Incrementally load 5 new rows per run
    WHERE row_num > (SELECT COUNT(*) FROM {{ this }}) 
      AND row_num <= ((SELECT COUNT(*) FROM {{ this }}) + 5)
{% else %}
    -- For the first full load, load the first 5 rows
    WHERE row_num <= 5
{% endif %}
