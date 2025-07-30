{{
    config(
        materialized='incremental',
        unique_key='ticket_id'
    )
}}

WITH updated_tickets AS (
    SELECT 
        t.ticket_id,
        t.customer_id,
        t.ticket_status,
        t.created_at,
        t.updated_at
    FROM {{ ref('customer_tickets') }} t
    INNER JOIN {{ this }} existing
        ON t.ticket_id = existing.ticket_id
    WHERE t.updated_at > existing.updated_at
)

SELECT * FROM updated_tickets

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}