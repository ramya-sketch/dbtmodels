with consolidated_vw as (
SELECT FP.property_id,FP.rental_income,FP.purchase_price,FP.sale_price,FP.profit,PI.size,PI.purchase_date
FROM {{ ref('rd_financial_performance') }} FP
JOIN {{ ref('rd_property_information') }} PI on FP.property_id = PI.property_id
)
select * from consolidated_vw