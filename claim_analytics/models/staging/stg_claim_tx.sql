select *
from {{ source('ztest', 'CLAIM_TX') }}
