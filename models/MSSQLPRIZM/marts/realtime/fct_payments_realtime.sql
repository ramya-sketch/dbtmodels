{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

select
    payment_id,
    order_id,
    amount,
    payment_status,
    event_time
from {{ ref('stg_rt_payments') }}
