{{ config(
    materialized = 'incremental',
    unique_key = 'CLAIM_ID',
    tags = ['fact']
) }}

with claims as (
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
    from {{ ref('stg_claims') }}

    {% if is_incremental() %}
        where UPDATED_DATE > (select max(UPDATED_DATE) from {{ this }})
    {% endif %}
),

claim_tx as (
    select
        CLAIM_ID,
        TX_ID,
        TX_DATE,
        TX_AMOUNT,
        TX_TYPE,
        CREATED_DATE as TX_CREATED_DATE
    from {{ ref('stg_claimstx') }}

    {% if is_incremental() %}
        where CREATED_DATE > (select max(TX_CREATED_DATE) from {{ this }})
    {% endif %}
)

select
    c.CLAIM_ID,
    c.CLAIM_NUMBER,
    c.POLICY_NUMBER,
    c.CLAIM_DATE,
    c.CLAIM_TYPE,
    c.STATE,
    c.STATUS,
    c.REPORTED_DATE,
    c.ACCIDENT_DATE,

    tx.TX_ID,
    tx.TX_DATE,
    tx.TX_AMOUNT,
    tx.TX_TYPE,

    c.CREATED_DATE as CLAIM_CREATED_DATE,
    c.UPDATED_DATE,
    tx.TX_CREATED_DATE

from claims c
left join claim_tx tx
    on c.CLAIM_ID = tx.CLAIM_ID
