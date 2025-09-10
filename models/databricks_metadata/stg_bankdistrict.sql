with source as (
    select * from {{ source('dqlabs', 'bankdistricts') }}
) 
select * from source
