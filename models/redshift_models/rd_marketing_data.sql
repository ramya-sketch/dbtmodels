with marketing_data_vw as (
    Select * FROM dbt_dqlabs.marketing_data_table
)
select * from marketing_data_vw