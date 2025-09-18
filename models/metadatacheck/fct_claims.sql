{{ 
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='delete+insert'
    ) 
}}

with claims as (
    select
        c.claim_id,
        c.accident_date,
        c.claim_date,
        c.status,
        c.claim_type,
        c.state,
        -- Simple calculated field
        datediff(day, c.accident_date, c.claim_date) as days_to_report_claim
    from {{ ref('stg_claims') }} c
),

transactions as (
    select
        t.tx_id,
        t.claim_id,
        t.tx_type,
        t.tx_amount
    from {{ ref('stg_transactions') }} t
),

joined as (
    select
        c.claim_id,
        c.accident_date,
        c.claim_date,
        c.status,
        c.claim_type,
        c.state,
        c.days_to_report_claim,
        t.tx_id,
        t.tx_type,
        t.tx_amount,
        -- Complex calculated field
        case 
            when c.status = 'Open' and c.state = 'CA' and t.tx_type = 'Repair'
            then t.tx_amount
            else 0
        end as open_claim_tx_amount_specific,
        row_number() over (partition by c.claim_id order by t.tx_id desc) as row_num
    from claims c
    left join transactions t
        on c.claim_id = t.claim_id
)

select
    claim_id,
    accident_date,
    claim_date,
    status,
    claim_type,
    state,
    days_to_report_claim,
    tx_id,
    tx_type,
    tx_amount,
    open_claim_tx_amount_specific
from joined

{% if is_incremental() %}
    -- Incrementally load 100 new rows per run
    where row_num > (select count(*) from {{ this }})
      and row_num <= ((select count(*) from {{ this }}) + 100)
{% else %}
    -- For the first full load, load the first 100 rows
    where row_num <= 100
{% endif %}
