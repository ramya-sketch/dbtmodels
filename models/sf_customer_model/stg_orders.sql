with stg_orders as (
    SELECT * FROM DQLABS_QA.DBT_CORE.STG_ORDERS_1
)
select * from stg_orders