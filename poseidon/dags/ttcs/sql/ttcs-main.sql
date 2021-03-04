SELECT ACCT_STAT.*,
NAICS.DESCRIPTION AS NAICS_DESCRIPTION
FROM
(SELECT
ACCOUNT.BUSINESS_OWNER_NAME,
ACCOUNT.OWNERSHIP_TYPE,
ACCOUNT.ACCOUNT_KEY,
ACCOUNT.PRIMARY_NAICS AS NAICS_CODE,
ACCOUNT.EXPIRATION_DT AS CERT_EXP_DT,
ACCOUNT.EFFECTIVE_DT AS CERT_EFF_DT,
ACCOUNT.ACCOUNT_STATUS AS ACCOUNT_STATUS_CODE,
ACCOUNT.CREATION_DT,
ACCOUNT.BUS_START_DT,
ACCOUNT.NUM_EMPLOYEES,
ACCOUNT.HOME_BASED_IND,
ACCOUNT.DO_NOT_PUBLISH_IND,
ACCOUNT.FEE_STATUS,
ACCOUNT.ORIGIN,
ACCOUNT.ONLINE_BILLING_EMAIL,
ACCOUNT_STATUS_VT.DESCRIPTION AS ACCOUNT_STATUS
FROM ACCOUNT
LEFT JOIN ACCOUNT_STATUS_VT
ON ACCOUNT.ACCOUNT_STATUS = ACCOUNT_STATUS_VT.ACCOUNT_STATUS
WHERE ACCOUNT.ACCOUNT_TYPE='BTAX'
AND (ACCOUNT.ACCOUNT_STATUS='A' 
	OR ACCOUNT.ACCOUNT_STATUS='I' 
	OR ACCOUNT.ACCOUNT_STATUS='P' 
	OR ACCOUNT.ACCOUNT_STATUS='C' 
	OR ACCOUNT.ACCOUNT_STATUS='N')) ACCT_STAT
LEFT JOIN NAICS
ON ACCT_STAT.NAICS_CODE = NAICS.NAICS
