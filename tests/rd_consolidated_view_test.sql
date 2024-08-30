{{ config(store_failures = true) }}
select
    property_id,
    sum(rental_income) as total_amount
from {{ ref('rd_consolidated_vw') }}
group by 1
having total_amount > 0