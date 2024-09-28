{{ 
config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='insert_overwrite'
)
}}

with customer_data as {
    select * from CUSTOMER_ID, FIRST_NAME, LAST_NAME, ORDER_ID, STATUS, ORDER_DATE
    FROM DQLABS_QA.DBT_CORE.STG_CUSTOMER CUSTOMER
    JOIN DQLABS_QA.DBT_CORE.STG_ORDERS ORDER ON CUSTOMER.CUSTOMER_ID=ORDER.CUSTOMER_ID
}
SELECT * FROM CUSTOMER_DATA LIMIT 50