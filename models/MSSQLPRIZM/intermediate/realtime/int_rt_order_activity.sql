{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

-- Step 1: Deduplicate and get latest order event
with latest_orders as (
    select
        *,
        row_number() over (
            partition by order_id
            order by ingested_at desc
        ) as rn
    from {{ ref('stg_rt_orders') }}
),

order_latest as (
    select
        order_id,
        customer_id,
        status as latest_status,
        ingested_at as last_order_event_time
    from latest_orders
    where rn = 1
),

-- Step 2: Join payments to get total paid per order
order_with_payments as (
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
)

-- Step 3: Incremental insert
select *
from order_with_payments
{% if is_incremental() %}
where last_order_event_time > (
    select coalesce(max(last_order_event_time), '1900-01-01 00:00:00') 
    from {{ this }}
)
{% endif %}
