{{ config(
    materialized='incremental',
    schema='report',
    incremental_strategy='merge',
    unique_key=['txn_date', 'account_id'],
    on_schema_change='sync_all_columns'
) }}

with activity_source as (

    select *
    from {{ ref('stg_daily_account_activity') }}

    {% if is_incremental() %}
        where load_ts >= (
            select dateadd(
                day,
                -3,
                coalesce(
                    max(load_ts),
                    '1900-01-01'::timestamp_ltz
                )
            )
            from {{ this }}
        )
    {% endif %}
),

activity as (

    select *
    from activity_source
    where txn_date is not null
      and account_id is not null

    qualify row_number() over (
        partition by txn_date, account_id
        order by load_ts desc
    ) = 1
),

changed_keys as (

    select txn_date, account_id
    from activity
),

txn_daily_rollup as (

    select
        t.txn_date,
        t.account_id,
        sum(t.txn_amount) as computed_txn_amount,
        count(t.txn_id)   as computed_txn_count

    from {{ ref('stg_banking_transactions') }} t

    inner join changed_keys k
        on t.txn_date = k.txn_date
       and t.account_id = k.account_id

    group by
        t.txn_date,
        t.account_id
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