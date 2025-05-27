{{ config(materialized='view') }}

WITH base_customer AS (
    SELECT * FROM {{ ref('customer_full') }}
),

ticket_summary AS (
    SELECT
        customer_id,
        COUNT(*) AS total_tickets,
        MAX(ticket_date) AS last_ticket_date
    FROM {{ ref('customer_tickets') }}
    GROUP BY customer_id
)

SELECT
    c.*,
    COALESCE(t.total_tickets, 0) AS total_tickets,
    t.last_ticket_date
FROM base_customer c
LEFT JOIN ticket_summary t
    ON c.customer_id = t.customer_id
