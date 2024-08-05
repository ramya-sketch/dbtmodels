with financial_performance_vw as (
    Select * FROM dbt_dqlabs.financial_performance_table
)
select * from financial_performance_vw