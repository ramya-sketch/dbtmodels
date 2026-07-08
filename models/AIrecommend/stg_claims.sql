{{ config(
    materialized = 'table' 
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    CLAIM_DATE,
    STATUS,
    accident_date, 
    created_date, 
    reported_date
from DQLABS_QA.ZTEST.CLAIM
