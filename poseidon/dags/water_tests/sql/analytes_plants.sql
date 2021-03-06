SELECT SAMPLE.SAMPLE_DATE,
SAMPLE.SOURCE,
XR_SOURCE.DESCRIPTION,
SAMPLE.SAMPLE_ID,
RESULT.ANALYTE,
RESULT.TEST_NUMBER,
RESULT.QUALIFIER,
RESULT.VALUE,
RESULT.UNITS,
SAMPLE.PROJECT_ID
FROM WPL.RESULT RESULT, 
WPL.SAMPLE SAMPLE, 
WPL.XR_SOURCE XR_SOURCE
WHERE RESULT.SAMPLE_ID = SAMPLE.SAMPLE_ID 
AND SAMPLE.SOURCE = XR_SOURCE.SOURCE 
AND ((RESULT.TEST_TYPE='SAMP') 
AND (XR_SOURCE.VALID='Y') AND (XR_SOURCE.SOURCE_NAME='SYS') 
AND (SAMPLE.RECEIVED=1) AND (SAMPLE.PROJECT_ID In ('FLUORIDE','PLANTS')) 
AND (RESULT.REPORTABLE=1)
