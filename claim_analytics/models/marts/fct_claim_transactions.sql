with claims as (
    select * from {{ ref('stg_claim') }}
),

tx as (
    select * from {{ ref('stg_claim_tx') }}
)

select
    c.CLAIM_ID,
    tx.TX_ID,
    c.CLAIM_NUMBER,
    c.POLICY_NUMBER,
    c.CLAIM_TYPE,
    c.STATUS,
    c.STATE,
    c.CLAIM_DATE,
    c.ACCIDENT_DATE,
    c.REPORTED_DATE,
    c.UPDATED_DATE as claim_updated_date,
    tx.TX_DATE,
    tx.TX_AMOUNT,
    tx.TX_TYPE,
    tx.CREATED_DATE as tx_created_date,
    current_timestamp() as load_ts
from claims c
left join tx
    on c.CLAIM_ID = tx.CLAIM_ID
