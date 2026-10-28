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
        where aml_load_ts >= (
            select coalesce(
                max(aml_load_ts),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )
    {% endif %}
),

activity_latest as (
    select *
    from activity_source
    where txn_date is not null
      and account_id is not null
    qualify row_number() over (
        partition by txn_date, account_id
        order by aml_load_ts desc
    ) = 1
)

select
    a.txn_date,
    a.account_id,
    d.account_type,
    d.branch_id,
    d.customer_id,
    d.customer_name,
    d.risk_rating,
    d.customer_segment,
    d.country,
    a.suspicious_amount,
    a.txn_amount,
    a.txn_count,
    a.aml_load_ts
from activity_latest a
left join {{ ref('stg_accounts_customers') }} d
    on a.account_id = d.account_id
where a.suspicious_amount is not null
  and a.suspicious_amount > 0