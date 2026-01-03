{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

with source_data as (
    select *
    from {{ source('raw_dbt', 'payments_stream') }}
    where order_id is not null
      and amount >= 0
      and event_time <= current_timestamp()
)

select *
from source_data
{% if is_incremental() %}
where event_time > (select max(event_time) from {{ this }})
{% endif %}
