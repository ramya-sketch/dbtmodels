with stg_claimstx as (
    select TX_AMOUNT, TX_TYPE
    from DQLABS_QA.ZTEST.CLAIM_TX
)
select * from stg_claimstx