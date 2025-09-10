with source as (
    select * from {{ source('dqlabs', 'bankdistrict') }}
) 
select * from source
