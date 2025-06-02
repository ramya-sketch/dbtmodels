{{ config(
    materialized='table'
) }}

with test_stg_customers as (
    SELECT * FROM DQLABS_QA.DBT_CORE.STG_CUSTOMERS
)
select * from test_stg_customers