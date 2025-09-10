-- models/bigquery/bq_bankaccount.sql
{{ config(materialized='table') }}

select * from {{ ref('stg_bankaccount') }}

