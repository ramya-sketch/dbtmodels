{{ config(
    materialized='table',
) }}
select *
from DQLABS_QA.ZTEST.CLAIM_TXs

