select
    account_id
from {{ ref('stg_accounts_customers') }}
where account_id in (
    'ACC2026091020433100013',
    'ACC2026120220433100021',
    'ACC2026061120433100006',
    'ACC2026051420433100028',
    'ACC2026071220433100009'
)