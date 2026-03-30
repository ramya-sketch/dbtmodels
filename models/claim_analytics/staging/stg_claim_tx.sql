{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID', 'TX_ID'],
    incremental_strategy='merge',
) }}

select *
from {{ source('ztest', 'CLAIM_TX') }}
{% if is_incremental() %}
where coalesce(UPDATED_DATE, CREATED_DATE, to_timestamp_ntz('1900-01-01'))
    >= (
        select coalesce(
            max(coalesce(UPDATED_DATE, CREATED_DATE)),
            to_timestamp_ntz('1900-01-01')
        )
        from {{ this }}
    )
{% endif %}
