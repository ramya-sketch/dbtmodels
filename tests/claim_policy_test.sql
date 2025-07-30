{{ config(store_failures = true) }}
select
    CUSTOMERID,
    count(CLAIMAMOUNT) as total_amount
from {{ ref('join') }}
group by 1
having total_amount > 0