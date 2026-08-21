select
    account_id,
    account_status
from {{ ref('stg_accounts_customers') }}
where account_status not in ('Active', 'Inactive', 'Closed')
   or account_status is null