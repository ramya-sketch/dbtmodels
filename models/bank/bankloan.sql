{{ config(materialized='view') }}
SELECT 
   city,
   state,
   divison
FROM {{ ref('bankdistricttable') }}