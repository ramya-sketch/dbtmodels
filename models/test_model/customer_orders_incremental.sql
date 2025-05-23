{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    c.first_name,
    c.last_name
from DQLABS_QA.DBT_CORE.STG_ORDERS o
join DQLABS_QA.DBT_CORE.STG_CUSTOMERS c
    on o.customer_id = c.customer_id
qualify row_number() over (partition by o.order_id order by o.order_date desc) = 1