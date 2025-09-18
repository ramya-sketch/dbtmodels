with accounts as (
    select * from {{ ref('stg_bankaccount') }}
),

districts as (
    select * from {{ ref('stg_bankdistrict') }}
)

select
    a.ACCOUNT_ID,
    a.DISTRICT_ID,
    d.CITY,
    d.STATE_NAME,
    d.STATE_ABBREV,
    d.DIVISION,
    a.FREQUENCY,
    a.PARSEDDATE,
    a.DAY,
    a.MONTH,
    a.YEAR
from accounts a
left join districts d
    on a.DISTRICT_ID = d.DISTRICT_ID
