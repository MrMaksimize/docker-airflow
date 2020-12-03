## About this job

This job creates the dataset of CRB cases published at [https://data.sandiego.gov/datasets/crb-cases/](https://data.sandiego.gov/datasets/crb-cases/).

Data source: Community Review Board on Police Practices Excel case tracker (shared drive)

Owning department: Neighborhood Services

SME: Sharmaine Moseley, smoseley@sandiego.gov

Date last updated: Tue Nov 24 2020

## To Dos

Solve upload problem. A file name cannot easily be specified in file transfer operator, and the files must be uploaded manually.

## Data transformations

This job should be kicked off manually, so it does not have a schedule (and currently it needs to be run locally until the upload problem can be solved). This is because of the sensitive nature of the data and inconsistencies in data entry year over year.

The first step is to review the source Excel file and ensure that data was entered in a way that will work with the function to process the Excel. The second step is to update the environment variable for the name of the Excel workbook in AWS Parameter Store.

### Read in the correct sheet

The code uses regex to get the fiscal year from the file name specified in the parameter store variable and uses that to find a sheet named FYXX or FY20XX. The Excel workbooks have all kinds of extra sheets, and the naming convention from year to year is not consistent.

Each row of the Excel is an allegation and officer in a case. One case can have multiple officers involved in multiple allegations. There may be blank cells where case-level information is not filled in for all allegation/officer rows. Usually, whomever is maintaining the Excel workbook and entering data uses cell colors to convey information.

Out of the fields we publish, below is how they can be divided along case/allegation lines.

Fields that contain case-level information include:

- The record number (#)
- Case
- Team
- Assigned date
- Completed date
- Presented date
- Days
- All of the days or less fields
- Changes
- PD division
- Whether CRB viewed body camera footage
- Complainant race
- Complainant gender

Fields that contain allegation/officer information include:

- Allegation
- IA finding
- CRB decision
- Vote
- Unanimous vote
- Officer
- BWC ON/OFF

### Separate cases, allegations, and bwc data for officers

This became necessary to avoid confusion about body worn cameras. There are two fields related to body-worn cameras: one with a yes/no depending on whether the CRB reviewed any footage while considering the case, and one with a yes if the officer had his or her camera turned on during the incident. The first field pertains only to the case, and the second field pertains only to the officer. It became necessary to have a dataset of cases that includes the field BWC Viewed by CRB Team, a dataset of allegations that has no body-worn camera information, and a dataset of (anonymized) officers with the field BWC ON/OFF.

### Anonymize officer

Officer names are replaced with an anonymous person id (pid). Officers are only tracked within cases and not across cases, so if an officer has multiple allegations for a single case, all those allegations will have the same pid. The pid is assigned by getting a unique list of officers involved in a case (in order of data entry) and numbering them, starting with 1. This is then joined back to the original case records on case number and officer name, then officer name is dropped.

### Omit fields that contain information that is not public

This is very important. Do not change this without speaking to SME. When the Excel is first loaded, it is divided into cases and allegations by selecting fields TO INCLUDE. This way, we will not forget to DROP fields we should not publish.

### Final field name map

| Field in FY2020 Excel      | Field in prod file                   |
|----------------------------|--------------------------------------|
| #                          | id                                   |
| Officer's Name             | pid (anonymized)                     |
| Case                       | case_number                          |
| Team                       | team                                 |
| Assigned                   | date_assigned                        |
| Completed                  | date_completed                       |
| Presented                  | date_presented                       |
| Days                       | days_number                          |
| 30 days or less            | days_30_or_less                      |
| 60 days or less            | days_60_or_less                      |
| 90 days or less            | 90_days_or_less                      |
| 120 days or less           | 120_days_or_less                     |
| Allegation                 | allegation                           |
| IA Finding                 | ia_finding                           |
| CRB Decision               | crb_decision                         |
| Changes                    | changes                              |
| Vote                       | vote                                 |
| Unanimous Vote             | unanimous_vote                       |
| Incident Address           | *dropped*                            |
| PD Division                | pd_division                          |
| BWC Viewed by CRB Team     | crb_viewed_bwc                       |
| BWC ON/OFF                 | *dropped* (added to another dataset) |
| Complainant's Name         | *dropped*                            |
| Race (complainant)         | complainant_race                     |
| Gender (complainant)       | complainant_gender                   |
| Race (officer)             | *dropped*                            |
| Gender (officer)           | *dropped*                            |
| Years of Service (officer) | *dropped*                            |

## Notes from SME


