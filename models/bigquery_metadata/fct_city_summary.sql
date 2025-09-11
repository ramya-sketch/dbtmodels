{{ config(materialized='table') }}

select
    STATE_NAME,
    count(distinct CITY) as total_cities,
    count(distinct DISTRICT_ID) as total_districts
from {{ ref('bankdistricts') }}
group by STATE_NAME
