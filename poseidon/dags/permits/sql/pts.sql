select a.approval_id,
  a.APPROVAL_TYPE_ID approval_type_id,
  atv.short_description short_desc,
  atv.description approval_type,
  atv.approval_process_cd appr_proc_code,
  atv.category_cd cat_code,
  atv.approval_authority authority,
  atv.days_to_expire_qty appl_days,
  asvt.description approval_status,
  a.ISSUE_DT approval_issue_dt,
  a.CLOSE_DT approval_close_dt,
  j.job_id job_id,
  p.PROJ_ID proj_id,
  p.DEVEL_NUM devel_id,
  p.title proj_title,
  p.scope proj_scope,
  p.job_order_id proj_job_order,
  p.APPL_DT proj_appl_date,
  p.DEEMED_COMPLETE_DT proj_deemed_cmpl_date,
  pjav.longitude job_lng,
  pjav.latitude job_lat,
  PJAV.ASSESSOR_PARCEL_10 job_apn,
  pjav.street_address job_address,
  PJAV.community_plan_id com_plan_id,
  pjav.community_plan_name as com_plan,
  afi.fee_type_unit_id as fee_type_unit_id,
  afi.fee_amt as fee_amt,
  afi.fee_desc as fee_desc,
  afi.fee_desc_abbr fee_type

  FROM p2k.approval a
  LEFT JOIN p2k.approval_type_vt atv ON a.approval_type_id = atv.approval_type_id
  LEFT JOIN p2k.approval_status_vt asvt ON a.approval_status_id = asvt.approval_status_id
  LEFT JOIN p2k.job j on a.job_id = j.job_id
  LEFT JOIN p2k.project p on j.proj_id = p.proj_id
  -- bring in pjav with com plan references
  LEFT JOIN (
        SELECT
        pj.community_plan_id,
        pj.job_id, pj.street_address,
        pj.assessor_parcel_10,
        pj.latitude,
        pj.longitude,
        cpvt.DESCRIPTION community_plan_name
        FROM p2k.primary_job_address_view pj
        LEFT JOIN site.community_plan_vt cpvt on pj.COMMUNITY_PLAN_ID = cpvt.COMMUNITY_PLAN_ID
  ) pjav on a.job_id = pjav.job_id
  -- bring in approval_feetype_unity

  LEFT JOIN(
        SELECT
        afu.APPROVAL_ID,
        afu.fee_type_unit_id,
        afu.amt fee_amt,
        afuv.description fee_desc,
        afuv.fee_type_unit_cd as fee_desc_abbr,
        afuv.USE_FEETYPEUNIT_AMT_IND
        FROM p2k.approval_feetypeunit afu
        LEFT JOIN p2k.fee_type_unit_vt afuv on afu.fee_type_unit_id = afuv.fee_type_unit_id

  ) afi on a.approval_id = afi.approval_id


