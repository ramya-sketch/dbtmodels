-- gold_electronics_sales.sql
WITH fct_sales AS (
    -- Referring to the fact model for electronics sales
    SELECT * 
    FROM {{ ref('fct_electronics_sales') }}
)
SELECT 
    CUSTOMER_NAME,
    SALE_DATE,
    TIMESTAMP,
    SALE_AMOUNT,
    PRODUCT_NAME
FROM fct_sales
WHERE SALE_DATE IS NOT NULL;
