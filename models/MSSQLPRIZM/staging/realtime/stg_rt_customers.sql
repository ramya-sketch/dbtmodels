{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select
    customer_id,
    name,
    email,
    updated_at,
    ingested_at
from {{ source('dbt_raw', 'customers_stream') }}
where customer_id is not null

{% if is_incremental() %}
  and ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
