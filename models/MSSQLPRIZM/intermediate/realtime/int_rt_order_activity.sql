{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with latest_orders as (
    select *,
           row_number() over (
             partition by order_id
             order by event_time desc
           ) as rn
    from {{ ref('stg_rt_orders') }}
    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

order_latest as (
    select
        order_id,
        customer_id,
        status as latest_status,
        event_time as last_order_event_time
    from latest_orders
    where rn = 1
)

select
    o.order_id,
    o.customer_id,
    o.latest_status,
    o.last_order_event_time,
    sum(p.amount) as total_paid
from order_latest o
left join {{ ref('stg_rt_payments') }} p
  on o.order_id = p.order_id
group by
    o.order_id,
    o.customer_id,
    o.latest_status,
    o.last_order_event_time
