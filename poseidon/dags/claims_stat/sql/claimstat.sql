SELECT *

FROM

(SELECT ORGANIZATION_CODE,
ORGANIZATION_DESC, 
CLAIM_NUMBER, 
ADRESS1, 
ZIP_CODE, 
CITY, 
CLAIMANT_TYPE_DESC, 
CLAIMANT_REFERENCE2_CODE, 
"CLAIMANT_REFERENCE2_Desc", 
INCIDENT_DESC, 
INCIDENT_DATE, 
ADJUSTING_LOC_RECEIVED_DATE, 
ADD_DATE, 
CLOSED_DATE, 
CLAIMANT_STATUS_DESC,
PAID_BI, 
PAID_PD, 
PAID_EXPENSE, 
PAID_TOTAL, 
INCURRED_BI, 
INCURRED_PD, 
INCURRED_EXPENSE, 
INCURRED_TOTAL,
SUBSTR(ORGANIZATION_CODE,1,4) AS ORG_CODE_SUBSTR
FROM CLAIMSTAT.CLAIMSTAT)


WHERE ORG_CODE_SUBSTR = '2116' #TSW
OR ORG_CODE_SUBSTR = '1914' #PD
OR ORG_CODE_SUBSTR = '2000' #PUD

AND INCIDENT_DATE >=  date '2008-07-01'
ORDER BY INCIDENT_DATE DESC


