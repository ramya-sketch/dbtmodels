{{ config(materialized='view') }}

SELECT
    ticketid AS ticket_id,
    customerid AS customer_id,
    issuetype AS issue_type,
    description,
    ticketdate AS ticket_date,
    resolutionstatus AS resolution_status,
    firstname AS first_name,
    lastname AS last_name,
    email,
    phonenumber AS phone_number,
    joindate AS join_date,
    status,
    loyaltypoints AS loyalty_points
FROM DQLABS_QA.staging.customer_tickets
