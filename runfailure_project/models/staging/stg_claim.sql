select *
from {{ source('ztest', 'CLAIM') }}
