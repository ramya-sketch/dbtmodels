{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID','TX_ID'],
    incremental_strategy='merge'
) }}
 
with source_data as (
 
    select
        CLAIM_ID,
        CLAIM_NUMBER,
        POLICY_NUMBER,
        CLAIM_TYPE,
        STATUS,
        STATE,
        CLAIM_DATE,
        ACCIDENT_DATE,
        REPORTED_DATE,
        UPDATED_DATE
    from {{ ref('stg_claims') }}
 
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
 
),
 
final as (
 
    select
        s.CLAIM_ID,
        tx.TX_ID,
 
        s.CLAIM_NUMBER,
        s.POLICY_NUMBER,
        s.CLAIM_TYPE,
        s.STATUS,
        s.STATE,
 
        s.CLAIM_DATE,
        s.ACCIDENT_DATE,
        s.REPORTED_DATE,
        tx.TX_DATE,
        s.UPDATED_DATE,
 
        tx.TX_AMOUNT,
        tx.TX_TYPE,
 
        tx.TX_CREATED_DATE,
        current_timestamp() as LOAD_TS
 
    from source_data s
    left join claim_tx tx
        on s.CLAIM_ID = tx.CLAIM_ID
 
)
 
-- ✅ Apply incremental filter at FINAL level
select *
from final
 
{% if is_incremental() %}
where UPDATED_DATE > (select max(UPDATED_DATE) from {{ this }})
{% endif %}


