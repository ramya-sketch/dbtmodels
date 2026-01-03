{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select
    customer_id,
    name,
    email,
    updated_at
from {{ ref('stg_rt_customers') }}
