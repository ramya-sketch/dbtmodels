SELECT
    CITY,
    DISTRICT_ID
FROM {{ ref('bankdistrict') }}