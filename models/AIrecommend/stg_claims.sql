{{ config(
    materialized = 'table'   -- use table so data is materialized and testable
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    CLAIM_DATE,
    CLAIM_TYPE,
    STATE,
    STATUS,
    REPORTED_DATE,
    ACCIDENT_DATE,
    CREATED_DATE,
    UPDATED_DATE
from DQLABS_QA.ZTEST.CLAIM
