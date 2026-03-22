{{ config(
    materialized = 'table' 
) }}

select
    CLAIM_ID,
    POLICY_NUMBER,
    CLAIM_DATE,
    CLAIM_TYPE,
    STATUS,
    accident_date, 
    created_date
    reported_date
from DQLABS_QA.ZTEST.CLAIM
