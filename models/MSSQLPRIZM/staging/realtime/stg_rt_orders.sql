{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    event_id,
    order_id,
    customer_id,
    status,
    amount,
    event_time,
    ingested_at
from {{ source('dbt_raw', 'orders_stream') }}
where order_id is not null
  and amount >= 0
  and status in ('CREATED','CONFIRMED','CANCELLED','COMPLETED')

{% if is_incremental() %}
  and ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
