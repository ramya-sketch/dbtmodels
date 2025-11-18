{{ config(materialized='view') }}

with client as (
    select *
    from {{ ref('stg_bankclient') }}
),

loan as (
    select *
    from {{ ref('stg_bankloan') }}
)

select
    loan.loan_id,
    loan.account_id,
    loan.amount,
    loan.payments,
    loan.purpose,
    loan.status,
    client.CLIENT_ID,
    client.FIRST,
    client.LAST,
    client.EMAIL
from loan
left join client
    on loan.account_id = client.client_id
