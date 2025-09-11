with stg_claimtx as (select
    CLAIM_ID,
    CAST(TX_ID AS NUMBER) AS TX_Numb
from DQLABS_QA.ZTEST.CLAIM_TX)
select * from stg_claimtx