{{
  config(
    materialized='table',
    tags=['marts', 'fact', 'electronics_sales']
  )
}}

WITH electronics_sales AS (
  SELECT 
    RECORD_IDD,
    "DATE" as SALE_DATE,  -- Renamed to avoid issues with reserved keyword
    STORE_ID,
    PRODUCT_NAME,
    CATEGORY,
    QUANTITY,
    UNIT_PRICE,
    TOTAL_AMOUNT,
    CUSTOMER_ID,
    SALES_REP,
    REGION,

    -- Additional calculated fields
    QUANTITY * UNIT_PRICE as CALCULATED_TOTAL,
    CASE 
      WHEN TOTAL_AMOUNT != (QUANTITY * UNIT_PRICE) THEN 'PRICE_MISMATCH'
      ELSE 'PRICE_MATCH'
    END as PRICE_VALIDATION,

    -- Date dimensions
    EXTRACT(YEAR FROM "DATE") as SALE_YEAR,
    EXTRACT(MONTH FROM "DATE") as SALE_MONTH,
    EXTRACT(DAY FROM "DATE") as SALE_DAY,
    EXTRACT(QUARTER FROM "DATE") as SALE_QUARTER,

    -- Business logic
    CASE 
      WHEN QUANTITY >= 10 THEN 'BULK_ORDER'
      WHEN QUANTITY >= 5 THEN 'MEDIUM_ORDER'
      ELSE 'SMALL_ORDER'
    END as ORDER_SIZE,

    CASE 
      WHEN TOTAL_AMOUNT >= 1000 THEN 'HIGH_VALUE'
      WHEN TOTAL_AMOUNT >= 500 THEN 'MEDIUM_VALUE'
      ELSE 'LOW_VALUE'
    END as ORDER_VALUE_CATEGORY
  FROM {{ ref('stg_retail_sales') }}
  WHERE CATEGORY = 'Electronics'
    AND PRODUCT_NAME IS NOT NULL
    AND QUANTITY > 0
    AND UNIT_PRICE > 0
    AND TOTAL_AMOUNT > 0
)

SELECT 
  RECORD_ID,
  SALE_DATE,
  STORE_ID,
  PRODUCT_NAME,
  CATEGORY,
  QUANTITY,
  UNIT_PRICE,
  TOTAL_AMOUNT,
  CUSTOMER_ID,
  SALES_REP,
  REGION,
  CALCULATED_TOTAL,
  PRICE_VALIDATION,
  SALE_YEAR,
  SALE_MONTH,
  SALE_DAY,
  SALE_QUARTER,
  ORDER_SIZE,
  ORDER_VALUE_CATEGORY,

  -- Metadata
  CURRENT_TIMESTAMP() as LOADED_AT,
  '{{ invocation_id }}' as DBT_RUN_ID

FROM electronics_sales
