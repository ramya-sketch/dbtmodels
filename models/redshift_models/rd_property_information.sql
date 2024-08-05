with property_information_vw as (
    Select * FROM dbt_dqlabs.property_information_table
)
select * from property_information_vw