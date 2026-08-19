select
    account_id
from {{ ref('stg_accounts_customers') }}
where customer_id is null