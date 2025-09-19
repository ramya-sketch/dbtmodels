with source1 as (
    select
        ACCOUNT_ID,
        DAY,
        DISTRICT_ID,
        FREQUENCY,
        MONTH,
        PARSEDDATE,
        YEAR
    from {{ source('dqlabs', 'bankaccount') }}
)
select * from source1
