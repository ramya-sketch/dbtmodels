{{ config(
    materialized = 'table'   
) }}
select
    CLAIM_ID,
    cast(TX_ID as number) as TX_Numb
from DQLABS_QA.ZTEST.CLAIM_TX
