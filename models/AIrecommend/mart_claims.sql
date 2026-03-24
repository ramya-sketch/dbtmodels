{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

select
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    STATE,
    STATUS,

    count(TX_ID) as total_transactions,
    sum(TX_AMOUNT) as total_claim_amount,

    min(CLAIM_DATE) as claim_date,
    max(TX_DATE) as last_transaction_date

from {{ ref('fact_claims') }}
group by
    CLAIM_ID,
    CLAIM_NUMBER,
    POLICY_NUMBER,
    STATE,
    STATUS