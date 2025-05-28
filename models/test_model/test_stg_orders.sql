{{ config(
    materialized='table'
) }}

with test_stg_orders as (
    SELECT * FROM DQLABS_QA.DBT_CORE.STG_ORDERS
)
select * from test_stg_orders