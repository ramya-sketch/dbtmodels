<<<<<<< HEAD
-- {{ config(tags=["staging", "etl"]) }}
with
    employee_sales_vw as (
        select
            a.salesorderid,
            concat(a.companyname, a.legalform) as company_info,
            b.created_employee_email,
            to_date(b.createdat, 'YYYYMMDD') as created_date,
            b.currency,
            a.item_grossamount,
            a.item_netamount,
            a.item_taxamount,
            a.productid
        from dqlabs_qa.staging.sales_business a
        inner join dqlabs_qa.staging.sales_emp b on a.salesorderid = b.salesorderid
    )
select *
from employee_sales_vw
=======
{{ config(tags=["staging", "etl"]) }}

WITH employee_sales_vw AS  (
    SELECT 
    A.SALESORDERID,
    CONCAT(A.COMPANYNAME, A.LEGALFORM) AS COMPANY_INFO,
    B.CREATED_EMPLOYEE_EMAIL,
    TO_DATE(B.CREATEDAT, 'YYYYMMDD') AS CREATED_DATE,
    B.CURRENCY,
    A.ITEM_GROSSAMOUNT,
    A.ITEM_NETAMOUNT,
    A.ITEM_TAXAMOUNT,
    A.PRODUCTID 
FROM 
    DQLABS_QA.STAGING.SALES_BUSINESS A
INNER JOIN 
    DQLABS_QA.STAGING.SALES_EMP B
    ON A.SALESORDERID = B.SALESORDERID
)
SELECT * FROM employee_sales_vw
>>>>>>> 9478e17513a8eb14fec2820d81e1cc69f3f7dcd6
