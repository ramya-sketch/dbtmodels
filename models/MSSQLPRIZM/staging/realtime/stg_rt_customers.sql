{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID'
) }}

with latest_customers as (
    select *,
           row_number() over (
             partition by CUSTOMER_ID
             order by INGESTED_AT desc
           ) as rn
    from {{ source('raw_dbt', 'customers_stream') }}
)

select
    CUSTOMER_ID,
    NAME,
    EMAIL,
    UPDATED_AT,
    INGESTED_AT
from latest_customers
where rn = 1
{% if is_incremental() %}
and INGESTED_AT > (select max(INGESTED_AT) from {{ this }})
{% endif %}
