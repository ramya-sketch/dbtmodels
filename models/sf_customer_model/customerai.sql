{{ 
config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge'
)
}}

WITH CUSTOMER_DATA AS (
    SELECT 
        ORDERx.CUSTOMER_ID, 
        CUSTOMER.FIRST_NAME, 
        CUSTOMER.LAST_NAME, 
        ORDERx.ORDER_ID, 
        ORDERx.STATUS, 
        ORDERx.ORDER_DATE
    FROM {{ ref('stg_customer') }} CUSTOMER
    JOIN {{ ref('stg_orders') }} ORDERx 
        ON CUSTOMER.CUSTOMER_ID = ORDERx.CUSTOMER_ID
    {% if is_incremental() %}
        -- Assuming ORDER_DATE is the column indicating new records
        WHERE ORDERx.ORDER_DATE > (SELECT MAX(ORDER_DATE) FROM {{ this }})
    {% endif %}
)
SELECT * FROM CUSTOMER_DATA
