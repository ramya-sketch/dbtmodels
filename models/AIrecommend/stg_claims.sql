{{ config(
    materialized = 'table' 
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    CLAIM_DATE,
    CLAIM_TYPE,
    STATE,
    STATUS,
    ACCIDENT_DATE, 
    CREATED_DATE, 
    REPORTED_DATE,
    COUNTRY,
    STATE_ABV
from DQLABS_QA.ZTEST.CLAIM
