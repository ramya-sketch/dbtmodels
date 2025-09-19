{{ 
  config(
    materialized='incremental',
    unique_key='ORDER_ID',
    incremental_strategy='delete+insert'
  )
}}

WITH customer_data AS (
    SELECT 
        o.ORDER_ID, 
        o.CUSTOMER_ID, 
        c.FIRST_NAME, 
        c.LAST_NAME,
        ROW_NUMBER() OVER (ORDER BY o.ORDER_ID DESC) AS row_num
    FROM 
        {{ ref('stg_orders') }} o
    JOIN 
        {{ ref('stg_customer') }} c
      ON o.CUSTOMER_ID = c.CUSTOMER_ID
)

SELECT 
    ORDER_ID, 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME
FROM customer_data

{% if is_incremental() %}
    -- Load next 5 rows after the latest ORDER_ID already in target
    WHERE ORDER_ID > (SELECT COALESCE(MAX(ORDER_ID), 0) FROM {{ this }})
      QUALIFY row_num <= 5
{% else %}
    -- First load: only load first 5 rows
    QUALIFY row_num <= 5
{% endif %}
