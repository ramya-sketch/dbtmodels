{{ config(
    materialized='incremental',
    unique_key='CLAIM_ID',
    incremental_strategy='merge',
) }}

select *
from {{ source('ztest', 'CLAIM') }}
{% if is_incremental() %}
where coalesce(UPDATED_DATE, to_timestamp_ntz('1900-01-01'))
    >= (
        select coalesce(max(UPDATED_DATE), to_timestamp_ntz('1900-01-01'))
        from {{ this }}
    )
{% endif %}
