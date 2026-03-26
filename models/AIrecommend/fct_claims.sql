{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID','TX_ID'],
    incremental_strategy='merge'
) }}

with latest_claims as (

    select *
    from {{ ref('stg_claims') }}

    {% if is_incremental() %}
        where UPDATED_DATE > (select max(UPDATED_DATE) from {{ this }})
    {% endif %}

    qualify row_number() over (
        partition by CLAIM_ID 
        order by UPDATED_DATE desc
    ) = 1
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
    tx.TX_DATE,
    c.UPDATED_DATE,

    tx.TX_AMOUNT,
    tx.TX_TYPE,

    tx.TX_CREATED_DATE,
    current_timestamp as LOAD_TS

from latest_claims c
left join claim_tx tx
    on c.CLAIM_ID = tx.CLAIM_ID
