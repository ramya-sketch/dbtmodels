WITH Claim_CTE AS (
    SELECT 
        ClaimID,
        PolicyID,
        ClaimAmount,
        Status,
        ClaimDate,
        DESCRIPTION,
        REPORTEDBY,
        CLAIMTYPE
    FROM DQLABS_QA.SOURCE_DEMO.Claim
)
SELECT * FROM Claim_CTE