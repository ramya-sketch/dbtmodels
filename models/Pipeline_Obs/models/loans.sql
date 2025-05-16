{{ config(
    materialized='view',
    schema='banking_data_target'
) }}

SELECT * FROM banking_data_source.loans;

