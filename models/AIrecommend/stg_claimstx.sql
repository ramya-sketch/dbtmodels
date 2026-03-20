with stg_claimstx as (
    select TX_TYPE, TX_AMOUNT
    from DQLABS_QA.ZTEST.CLAIM_TX
)
select * from stg_claimstx