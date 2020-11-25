## About this job

This job creates the dataset of CRB cases published at [https://data.sandiego.gov/datasets/crb-cases/](https://data.sandiego.gov/datasets/crb-cases/).

Data source: Community Review Board on Police Practices Excel case tracker (shared drive)

Owning department: Neighborhood Services

SME: Sharmaine Moseley, smoseley@sandiego.gov

Date last updated: Tue Nov 24 2020

## To Dos

None

## Data transformations

This job should be kicked off manually, so it does not have a schedule. It may even need to be run locally if changes need to be made to the code to accommodate changes in data entry.

The first step is to review the source Excel file and ensure that data was entered in a way that will work with the function to process the Excel. The second step is to update the environment variable for the name of the Excel sheet in AWS Parameter Store.

### Read in the correct sheet

The code uses regex to get the fiscal year from the file name specified in the parameter store variable and uses that to find a sheet named FYXX or FY20XX.

Each row of the Excel is an allegation and officer in a case. One case can have multiple officers involved in multiple allegations. There may be blank cells where case-level information needs to be populated for each row of the case. Use Pandas fillna with forward-fill method.

Fields that contain case-level information include:

- The record number (#)
- Case
- Team
- Assigned date
- Completed date
- Presented date
- Days
- All of the days or less fields
- PD division
- Whether CRB viewed body camera footage
- Complainant race
- Complainant gender

Fields that contain allegation/officer information include:

- Allegation
- IA finding
- CRB decision
- Changes
- Vote
- Unanimous vote
- Officer

### Anonymize officer

Officer names are replaced with an anonymous person id (pid). Officers are only tracked within cases and not across cases, so if an officer has multiple allegations for a single case, all those allegations will have the same pid. The pid is assigned by getting a unique list of officers involved in a case (in order of data entry) and numbering them, starting with 1. This is then joined back to the original case records on case number and officer name, then officer name is dropped.

### Split off BWC turned on/off for a separate dataset

Whether the body-worn camera was turned on or off is per officer per case. If an officer has multiple allegations, the BWC on/off will be the same value for each. Because the cases dataset is per allegation per officer per case, and BWC on/off is per officer per case, this has been split off into a separate dataset.

### Remove fields that contain information that is not public

This is very important. Do not change this without speaking to SME.

### Final field name map

| Field in FY2020 Excel | Field in prod file |
|-----------------------|--------------------|
| # | id |
| Officer's Name | pid (anonymized) |
| Case | case_number |
| Team | team |
| Assigned | date_assigned |
| Completed | date_completed |
| Presented | date_presented |
| Days | days_number |
| 30 days or less | days_30_or_less |
| 60 days or less | days_60_or_less |
| 90 days or less | 90_days_or_less |
| 120 days or less | 120_days_or_less |
| Allegation | allegation |
| IA Finding | ia_finding |
| CRB Decision | crb_decision |
| Changes | changes |
| Vote | vote |
| Unanimous Vote | unanimous_vote |
| Incident Address | *dropped* |
| PD Division | pd_division |
| BWC Viewed by CRB Team | crb_viewed_bwc |
| BWC ON/OFF | *dropped* (added to another dataset) |
| Complainant's Name | *dropped* |
| Race (complainant) | complainant_race |
| Gender (complainant) | complainant_gender |
| Race (officer) | *dropped* |
| Gender (officer) | *dropped* |
| Years of Service (officer) | *dropped* |

## Notes from SME


