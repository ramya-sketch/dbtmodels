WITH customer_data_vw AS  (
    SELECT * FROM DQLABS_QA.REPORTING.CUSTOMER_DATA_DEMO
    WHERE  IS_ACTIVE = TRUE
    AND NOTES IN ('Note 1','Note 2')
)
SELECT * FROM customer_data_vw