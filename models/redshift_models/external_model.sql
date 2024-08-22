{{ config(materialized='table') }}
WITH people_data AS (
    SELECT
        id,
        first_name,
        last_name,
        email
    FROM
        {{ source('redshift_external', 'people_dbt_core') }}
)

SELECT
*
FROM
    people_data
