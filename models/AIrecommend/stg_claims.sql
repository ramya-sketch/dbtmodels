{{ config(
    materialized = 'table' 
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    CLAIM_DATE,
    CLAIM_TYPE,
    STATE,
    STATUS,
from DQLABS_QA.ZTEST.CLAIM
