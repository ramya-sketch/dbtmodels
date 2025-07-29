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
