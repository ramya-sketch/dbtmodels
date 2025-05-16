{{ config(
    materialized='view',
    schema='banking_data_target'
) }}

SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    l.loan_id,
    l.loan_amount,
    l.loan_status,
    t.transaction_id,
    t.transaction_date,
    t.amount AS transaction_amount
FROM 
    {{ ref('customers') }} c
LEFT JOIN 
    {{ ref('loans') }} l ON c.customer_id = l.customer_id
LEFT JOIN 
    {{ ref('transactions') }} t ON c.customer_id = t.customer_id
