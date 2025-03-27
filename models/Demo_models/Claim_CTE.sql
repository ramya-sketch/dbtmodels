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
    FROM DQLABS_POV.DQLABS.Claim
)
SELECT * FROM Claim_CTE