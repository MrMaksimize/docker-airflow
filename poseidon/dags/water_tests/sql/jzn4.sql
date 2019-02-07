-- Ecoli linked with field record ph
-- (F_FIELD_RECORD, F_ANALYTE, F_QUAL, F_VALUE)
SELECT DISTINCT
f.field_record as F_FIELD_RECORD, 
f.analyte as F_ANALYTE, 
f.qualifier as F_QUAL, 
f.value as F_VALUE
FROM
WPL.SAMPLE s, WPL.FIELD_DATA f, WPL.RESULT r
WHERE r.analyte in ('E_COLI')
AND f.analyte in ('PH')
AND s.source in (SELECT DISTINCT SOURCE FROM WPL.XR_SOURCE WHERE SOURCE_NAME ='SYS' AND VALID='Y')
AND SAMPLE_DATE BETWEEN '$ds' AND '$de'
AND s.sample_id=r.sample_id
AND s.field_record=f.field_record