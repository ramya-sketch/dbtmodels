{{
    config(
        materialized='incremental',
        unique_key='ticket_id'
    )
}}

WITH new_tickets AS (
    SELECT 
        ticket_id,
        customer_id,
        ticket_status,
        created_at,
        updated_at
    FROM {{ ref('customer_tickets') }}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
)

SELECT * FROM new_tickets

{% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}