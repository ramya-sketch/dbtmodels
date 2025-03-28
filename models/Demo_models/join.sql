WITH join_vw AS
(
SELECT 
    c.CLAIMID,
    c.POLICYID,
    p.CUSTOMERID,
    p.CUSTOMERNAME,
    p.CUSTOMEREMAIL,
    c.CLAIMAMOUNT,
    c.STATUS AS CLAIM_STATUS,
    c.CLAIMTYPE,
    c.REPORTEDBY,
    c.DESCRIPTION,
    p.POLICYTYPE,
    p.PREMIUMAMOUNT,
    p.STATUS AS POLICY_STATUS
FROM {{ ref('Claim_CTE') }} c
JOIN {{ ref('Policy_CTE') }} p 
    ON c.claimtype = p.polictype
)
SELECT * FROM join_vw