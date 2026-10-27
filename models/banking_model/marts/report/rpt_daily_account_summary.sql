{{ config(
    materialized='incremental',
    schema='report',
    incremental_strategy='merge',
    unique_key=['txn_date', 'account_id']
) }}

with txn_daily_rollup as (
    select
        txn_date,
        account_id,
        sum(txn_amount) as computed_txn_amount,
        count(txn_id)   as computed_txn_count
    from {{ ref('stg_banking_transactions') }}

    {% if is_incremental() %}
        where txn_date >= dateadd(
            day,
            -3,
            (
                select coalesce(max(txn_date), '1900-01-01'::date)
                from {{ this }}
            )
        )
    {% endif %}

    group by 1, 2
),

activity as (
    select *
    from {{ ref('stg_daily_account_activity') }}

    {% if is_incremental() %}
        where txn_date >= dateadd(
            day,
            -3,
            (
                select coalesce(max(txn_date), '1900-01-01'::date)
                from {{ this }}
            )
        )
    {% endif %}
)

select
    a.txn_date,
    a.account_id,
    a.txn_amount,
    a.txn_count,
    a.suspicious_amount,
    t.computed_txn_amount,
    t.computed_txn_count,
    a.load_ts,
    current_timestamp() as report_generated_at
from activity a
left join txn_daily_rollup t
    on a.txn_date = t.txn_date
    and a.account_id = t.account_id