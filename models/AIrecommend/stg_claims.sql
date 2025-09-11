-- models/broken_syntax.sql
with stg_claim as (select
    claim_id,
    accident_date   -- ❌ missing comma
from DQLABS_QA.ZTEST.CLAIMS)

select * from stg_claim