{{
    config(
        materialized='incremental',
        unique_key='ticket_id'
    )
}}

WITH current_tickets AS (
    SELECT ticket_id
    FROM {{ ref('customer_tickets') }}
),

deleted_tickets AS (
    SELECT 
        t.ticket_id,
        t.customer_id,
        t.ticket_status,
        t.created_at,
        t.updated_at,
        CURRENT_TIMESTAMP() as deleted_at
    FROM {{ this }} t
    LEFT JOIN current_tickets c
        ON t.ticket_id = c.ticket_id
    WHERE c.ticket_id IS NULL
)

SELECT * FROM deleted_tickets

{% if is_incremental() %}
    WHERE deleted_at > (SELECT MAX(deleted_at) FROM {{ this }})
{% endif %}