{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID','TX_ID']
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
    select *
    from (
        select
            CLAIM_ID,
            TX_ID,
            TX_DATE,
            TX_AMOUNT,
            TX_TYPE,
            CREATED_DATE as TX_CREATED_DATE,
            row_number() over (
                partition by CLAIM_ID, TX_ID
                order by CREATED_DATE desc
            ) as rn
        from {{ ref('stg_claimstx') }}
    ) t
    where rn = 1
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
        tx.TX_CREATED_DATE,
        tx.TX_AMOUNT,
        tx.TX_TYPE,
        current_timestamp() as LOAD_TS
    from source_data s
    left join claim_tx tx
        on s.CLAIM_ID = tx.CLAIM_ID
),

deduped as (
    select *
    from (
        select * from (
            select *,
                   row_number() over (
                       partition by CLAIM_ID, TX_ID
                       order by UPDATED_DATE desc
                   ) as rn
            from final
        ) inner_t
        where rn = 1
    ) t
)

select *
from deduped

{% if is_incremental() %}
    where UPDATED_DATE > (select coalesce(max(UPDATED_DATE), '1900-01-01') from {{ this }})
{% endif %}