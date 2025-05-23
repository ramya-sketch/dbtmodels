{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

{% if is_incremental() %}

merge into {{ this }} as target
using (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        c.first_name,
        c.last_name
    from DQLABS_QA.DBT_CORE.STG_ORDERS o
    join DQLABS_QA.DBT_CORE.STG_CUSTOMERS c
        on o.customer_id = c.customer_id
) as source
on target.order_id = source.order_id

when matched and (
    target.customer_id != source.customer_id or
    target.order_date != source.order_date or
    target.status != source.status or
    target.first_name != source.first_name or
    target.last_name != source.last_name
)
then update set
    customer_id = source.customer_id,
    order_date = source.order_date,
    status = source.status,
    first_name = source.first_name,
    last_name = source.last_name

when not matched then insert (
    order_id,
    customer_id,
    order_date,
    status,
    first_name,
    last_name
) values (
    source.order_id,
    source.customer_id,
    source.order_date,
    source.status,
    source.first_name,
    source.last_name
);

{% else %}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    c.first_name,
    c.last_name
from DQLABS_QA.DBT_CORE.STG_ORDERS o
join DQLABS_QA.DBT_CORE.STG_CUSTOMERS c
    on o.customer_id = c.customer_id

{% endif %}
