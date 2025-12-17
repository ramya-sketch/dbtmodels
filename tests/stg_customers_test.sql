select *
from {{ ref('stg_customers') }}
where trim(first_name) = '' or NULL
   or trim(last_name) = ''