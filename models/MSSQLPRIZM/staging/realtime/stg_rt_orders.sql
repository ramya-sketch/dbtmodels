{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with source_data as (
    select *
    from {{ source('raw_dbt', 'orders_stream') }}
    where order_id is not null
      and amount >= 0
      and status in ('CREATED', 'CONFIRMED', 'SHIPPED', 'DELIVERED')
      and event_time <= current_timestamp()
)

select *
from source_data
{% if is_incremental() %}
where event_time > (select max(event_time) from {{ this }})
{% endif %}
