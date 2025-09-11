-- models/broken_ref.sql
select *
from {{ ref('stg_claimtx') }}
