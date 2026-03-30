{{ config(
    materialized='incremental',
    unique_key=['CLAIM_ID', 'TX_ID'],
    incremental_strategy='merge',
) }}

select *
from {{ source('ztest', 'CLAIM_TX') }}
{# CLAIM_TX has no UPDATED_DATE; use row / line timestamps from the source. #}
{% if is_incremental() %}
where coalesce(CREATED_DATE, TX_DATE, to_timestamp_ntz('1900-01-01'))
    >= (
        select coalesce(
            max(coalesce(CREATED_DATE, TX_DATE)),
            to_timestamp_ntz('1900-01-01')
        )
        from {{ this }}
    )
{% endif %}
