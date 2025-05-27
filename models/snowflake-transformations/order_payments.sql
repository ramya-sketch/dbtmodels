{{ config(materialized='view') }}

SELECT
    orderitemid AS order_item_id,
    orderid AS order_id,
    productname AS product_name,
    quantity,
    priceperitem AS price_per_item,
    customerid AS customer_id,
    orderdate AS order_date,
    orderstatus AS order_status,
    totalamount AS total_amount,
    paymentid AS payment_id,
    paymentdate AS payment_date,
    paymentamount AS payment_amount,
    paymentmethod AS payment_method
FROM DQLABS_QA.staging.order_payments
