{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    event_id,
    payment_id,
    order_id,
    amount,
    payment_status,
    event_time,
    ingested_at
from {{ source('dbt_raw', 'payments_stream') }}
where payment_id is not null
  and amount >= 0
  and payment_status in ('SUCCESS','FAILED','PENDING')

{% if is_incremental() %}
  and ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
