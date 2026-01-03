{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    latest_status,
    total_paid,
    last_order_event_time
from {{ ref('int_rt_order_activity') }}
