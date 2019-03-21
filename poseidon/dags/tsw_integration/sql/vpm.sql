SELECT
swbmp.sw_bmp_report_id,
swp.sw_project_id project_id,
swp.project_name,
swbmp.state as bmpr_state,
swbmp.isNoticeViolation as is_nov,
swbmp.report_date,
swbmp.reinspection_date,
swp.permit_number,
swbmp.sw_inspection_type_id,
it.title,
swp.sw_section_id as section_id,
ss.title as section_title,
swp.location_street,
swp.location_city,
swp.location_state,
swp.location_zip,
swbmp.comments
from sw_bmp_report swbmp

LEFT JOIN sw_project swp on swbmp.sw_project_id = swp.sw_project_id
LEFT JOIN sw_inspection_type it on swbmp.sw_inspection_type_id = it.sw_inspection_type_id
LEFT JOIN sw_section ss on swp.sw_section_id = ss.sw_section_id
WHERE swbmp.isNoticeViolation = '1'
AND swp.sw_project_id is not NULL
AND swbmp.report_date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -3 year)
ORDER BY report_date desc