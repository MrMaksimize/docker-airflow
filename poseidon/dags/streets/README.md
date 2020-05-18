## About this job

This job creates the dataset of street repair projects published at [https://data.sandiego.gov/datasets/streets-repair-projects/](https://data.sandiego.gov/datasets/streets-repair-projects/). It also creates a dataset the streets team submits to Accela for conflict management.

Data source: Cartegraph

Owning department: Transportation & Storm Water Streets Division

SME: Chris Hudson, CHudson@sandiego.gov

Date last updated: Tue Jul 9 10:12:48 2019 -0700

## To Dos

- Delete streets_imcat_dags.py and streets_sdif_dags.py.
- Create different tasks for imcat versus portal datasets. Currently, these differences are handled with if statements in the main task.
- Delete Sonar tasks

## Data transformations

The data extract starts with a SQL query that joins records from wdwomaingeneral, pvevents, and pvmaingeneral tables. The SQL query makes use of multiple where clauses to filter records returned. See pavement_ex.sql in the sql folder.

### Project type

Various string values exist in the wo_proj_type field. The script overwrites the original values in this field to one of three new values: Concrete, Slurry, or Overlay.

### Job status

The wo_status field also has various string values that must be reset to either Construction or Post Construction. The wo_status field is not overwritten. Instead, a new status field is populated with one of the two new values.

### Project manager

The wo_pm and wo_pm_phone values are overwritten to one of five different new values depending on whether the job is UTLY, TSW, Slurry, Series Circuit, or Overlay/Concrete.

###  Creating start, end, and moratorium dates

- moratorium field is created and initially populated with job_end_dt values
- moratorium is reset to None if the project type is concrete
- moratorium is reset to None if job status is Post Construction
- start field is created and initially populated with wo_design_start_dt
- end field is created and initially populated with wo_design_end.
- start and end are overwritten to job_start_dt and job_end_dt if the project type is UTLY or TSW
- start and end are both overwritten to job_end_dt if job is completed and not UTLY or TSW project type

### Calculate paving miles

For segments with a width greater than or equal to 50 feet, the paving miles are length in feet * 2 / 5280. Otherwise, the paving miles are length in feet / 5280.

### Remove records

- where project type is UTLY and job_end_dt is null
- where job activity is data entry, mill, structure wid, or paving
- where the job_end_dt is not null and greater than 5 years ago
- where the job_end_dt is not null and greater than 3 years ago for Slurry project type
- where job activity, project type, or status are null

### Final field name map

| Columns in SQL | Columns in Accela data |Columns in public data |
| ------ | ------ | ------ |
| *N/A* | to_delete | *N/A* |
| *N/A* | status | *N/A* |
| *N/A* | paving_miles | paving_miles |
| *N/A* | start | date_start |
| *N/A* | end | date_end |
| *N/A* | moratorium | date_moratorium |
| pve_id | pve_id | pve_id |
| seg_id | seg_id | seg_id |
| rd_seg_id | rd_seg_id | *dropped* |
| wo_id | projectid | project_id |
| wo_name | title | title |
| wo_pm | pm | project_manager |
| wo_pm_phone | pm_phone | project_manager_phone |
| wo_design_start_dt | wo_design_start_dt | *dropped* |
| wo_design_end_dt | wo_design_end_dt | *dropped* |
| job_start_dt | job_start_dt | *dropped* |
| job_end_dt | job_end_dt | *dropped* |
| job_completed_cbox | completed | *dropped* |
| wo_status | wo_status | *dropped* |
| wo_proj_type | proj_type | type |
| job_activity | activity | *dropped* |
| wo_resident_engineer | resident_engineer | resident_engineer |
| street | street | address_street |
| street_from | street_from | street_from |
| street_to | street_to | street_to |
| job_entry_dt | entry_dt | *dropped* |
| job_updated_dt | last_update | *dropped* |
| seg_placed_in_srvc | seg_in_serv | *dropped* |
| seg_func_class | seg_fun | *dropped* |
| seg_council_district | seg_cd | *dropped* |
| seg_length_ft | length | length |
| seg_width_ft | width | width |

## Notes from SME

- Do not populate moratorium for Concrete activity types
- Keep TSW records where pve.completed is not checked. Set status to Construction by default for those records not complete.
- Populate start and end with completed date for records where pve.completed as job_completed_cbox is checked, status is in post construction
- For TSW work order, the job start and end dates should be used for the construction date range
- For UTLY work order, the job start and end dates should be used for the construction date range


