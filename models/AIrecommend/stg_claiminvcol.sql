with stg_claimtx as (select
    CLAIM_ID,
    TX_IDs 
from DQLABS_QA.ZTEST.CLAIM_TX)

select * from stg_claimtx

