## About this job

This job creates the dataset of street repair projects published at [https://data.sandiego.gov/datasets/streets-repair-projects/](https://data.sandiego.gov/datasets/streets-repair-projects/). It also creates a dataset the streets team submits to Accela for conflict management.

Data source: Cartegraph
Owning department: Transportation & Storm Water Streets Division
Contact person: Chris Hudson, CHudson@sandiego.gov

## To Dos

- Delete streets_imcat_dags.py and streets_sdif_dags.py.
- Create different tasks for imcat versus portal datasets. Currently, these differences are handled with if statements in the main task.
- Delete Sonar tasks

## Data transformations

The data extract starts with a SQL query that joins records from wdwomaingeneral, pvevents, and pvmaingeneral tables. The SQL query makes use of multiple where clauses to filter records returned. See pavement_ex.sql in the sql folder.

### Field map

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



