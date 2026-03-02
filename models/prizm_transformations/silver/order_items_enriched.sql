{{ 
    config(
        schema='SILVER',       
        materialized='table'  
    ) 
}}
SELECT
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID,
    oi.PRODUCT_ID,
    oi.QTY,
    oi.UNIT_PRICE,
    oi.DISCOUNT_AMT,
    oi.TOTAL_PRICE,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.BRAND
FROM {{ source('dqlabs_qa', 'ORDER_ITEMS') }} oi
LEFT JOIN {{ source('dqlabs_qa', 'PRODUCTS') }} p
    ON oi.PRODUCT_ID = p.PRODUCT_ID
