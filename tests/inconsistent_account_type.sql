select
    account_id
from {{ ref('stg_accounts_customers') }}
group by account_id
having count(distinct account_type) > 1