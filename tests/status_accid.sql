select account_id
from {{ ref('stg_accounts_customers') }}
where account_status = 'DORMANT'