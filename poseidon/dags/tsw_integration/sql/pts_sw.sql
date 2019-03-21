SELECT
insp.INSP_ID,
pj.assessor_parcel_10,
pj.latitude,
pj.longitude,
pj.STREET_ADDRESS,
insp.INSP_TYPE_ID,
insp_type.DESCRIPTION as INSP_TYPE_NM,
insp.INSP_RESULT_ID,
insp_result.DESCRIPTION as INSP_RESULT_NM,
insp.PERFORMED_END_DT,
prj.TITLE as PROJ_TITLE,
insp.SCOPE,
insp.LOCATION_NOTE,
insp.CONSTRUCTION_NOTE
FROM P2K.INSP insp
LEFT JOIN P2K.INSP_TYPE_VT insp_type ON insp.INSP_TYPE_ID = insp_type.INSP_TYPE_ID
LEFT JOIN P2K.INSP_STATUS_VT insp_status on insp.INSP_STATUS_ID = insp_status.INSP_STATUS_ID
LEFT JOIN P2K.INSP_RESULT_VT insp_result on insp.INSP_RESULT_ID = insp_result.INSP_RESULT_ID
LEFT JOIN P2K.INSP_GRP insp_grp on insp.INSP_GRP_ID = insp_grp.INSP_GRP_ID
LEFT JOIN P2K.APPROVAL appr on insp_grp.APPROVAL_ID = appr.approval_id
LEFT JOIN p2k.primary_job_address_view pj ON appr.job_id = pj.job_id
LEFT JOIN p2k.JOB jb on appr.JOB_ID = jb.JOB_ID
LEFT JOIN p2k.PROJECT prj on jb.PROJ_ID = prj.PROJ_ID

WHERE insp.INSP_TYPE_ID IN

(
SELECT INSP_TYPE_ID from P2K.INSP_TYPE_VT
WHERE DESCRIPTION LIKE '%water%' OR
DESCRIPTION LIKE '%storm%' OR
DESCRIPTION LIKE '%Water%' OR
DESCRIPTION LIKE '%Storm%' OR
DESCRIPTION LIKE '%BMP%' OR
DESCRIPTION LIKE '%drain%'
)
AND insp.INSP_STATUS_ID IN (6, 7) -- Inspections performed or Completed only
--AND ROWNUM <= 100
AND insp.INSP_RESULT_ID != 1
AND insp.PERFORMED_END_DT >= trunc(trunc(sysdate,'y')-3,'y')
ORDER BY insp.insp_id DESC
