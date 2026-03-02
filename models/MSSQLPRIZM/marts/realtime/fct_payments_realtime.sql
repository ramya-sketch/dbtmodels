{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

with latest_payments as (
    select 
        order_id,
        sum(amount) as total_paid,
        max(ingested_at) as last_payment_ingested_at
    from {{ ref('stg_rt_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.status as latest_status,
    o.event_time as last_order_event_time,
    p.total_paid,
    p.last_payment_ingested_at
from {{ ref('stg_rt_orders') }} o
left join latest_payments p
    on o.order_id = p.order_id
