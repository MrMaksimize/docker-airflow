SELECT   
         pve.pveventsoid as pve_id,
         pvm.id as seg_id,
         pvm.roadsegid as rd_seg_id,
         wo.id as wo_id,
         wo.workordername as wo_name,
         wo.projectmanager as wo_pm,
         wo.phone as wo_pm_phone,
         wo.designstartdate as wo_design_start_dt,
         wo.designenddate as wo_design_end_dt,
         pve.whenstarted as job_start_dt,
         pve.whenended as job_end_dt,
         pve.completed as job_completed_cbox,
         wo.status as wo_status,
         wo.projecttype as wo_proj_type,    
         pve.activity as job_activity,
         wo.residentengineer as wo_resident_engineer,
         pvm.streetname as street,
         pvm.firstcrossstreet as street_from,
         pvm.secondcrossstreet as street_to,
         pve.entrydate as job_entry_dt,
         pve.lastupdateddate as job_updated_dt,
         pvm.placedinservice as seg_placed_in_srvc,
         pvm.functionalclassification as seg_func_class,
         pvm.district as seg_council_district,
         pvm.length as seg_length_ft,
         pvm.pavementwidth as seg_width_ft
FROM     [dbo].wdwomaingeneral wo
LEFT OUTER JOIN [dbo].pvevents pve ON wo.id = pve.workorder
INNER JOIN [dbo].pvmaingeneral pvm ON pve.pvmaingeneraloid = pvm.pvmaingeneraloid
WHERE (
   wo.id LIKE 'FY1%' 
   OR wo.id LIKE 'AC%'
   OR wo.id LIKE 's1%'
   OR wo.id LIKE 'pcc%'
   OR wo.id LIKE 'ACR%'
   OR wo.id LIKE 's2%'
   OR wo.id LIKE 'SP'
   OR wo.id = 'TSW'
   OR wo.id = 'utly'
   OR wo.id = 'DMP1A'
   )
AND wo.id != 'FY10-S3M'
AND pvm.retired is null
--AND pvm.id = 'SS-032083'
ORDER BY wo.id ASC,
         wo.wdwomaingeneraloid ASC;
