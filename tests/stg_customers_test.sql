select *
from {{ ref('stg_customers') }}
where
      first_name is null
   or trim(first_name) = ''
   or last_name is null
   or trim(last_name) = ''