-- Use total coliform again and link to field data of temp
-- (F_FIELD_RECORD, F_ANALYTE, F_QUAL, F_VALUE)
SELECT DISTINCT f.field_record as F_FIELD_RECORD, 
f.analyte as F_ANALYTE, 
f.qualifier as F_QUAL,
f.value as F_VALUE
FROM
WPL.SAMPLE s, WPL.FIELD_DATA f, WPL.RESULT r
WHERE r.analyte in ('T_COLIFORM')
AND f.analyte in ('TEMPERATURE')
AND s.source in (SELECT DISTINCT SOURCE FROM WPL.XR_SOURCE WHERE SOURCE_NAME ='SYS' AND VALID='Y')
AND SAMPLE_DATE BETWEEN '$ds' AND '$de'
AND s.sample_id=r.sample_id
AND s.field_record=f.field_record