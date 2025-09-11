{{ config(
    materialized='incremental',
    unique_key='TX_ID'
) }}
select *
from DQLABS_QA.ZTEST.CLAIM_TX

