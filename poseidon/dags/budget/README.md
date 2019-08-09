## About this job

This job creates 10 datasets available at [https://data.sandiego.gov/datasets/?department=department-of-finance](https://data.sandiego.gov/datasets/?department=department-of-finance). These datasets match the budget data visualized at budget.sandiego.gov and do not necessarily match the data as it resides in its source system. When DOF performs an update to the online budget data - which involves manual quality checks by the department - they put the same data extracts into a shared drive folder to create these datasets.

Data source: SAP extracts stored in the DOF shared drive

Owning department: Department of Finance

SME: Jeremy Broadhead, JBroadhead@sandiego.gov

Date last updated: Tue Jul 9 10:12:48 2019 -0700

## To Dos

- Change schedule to None so job can be run ad-hoc when DOF notifies us new files are ready
- Include fiscal year column from CIP 2Date budget files

## Data transformations

Most of the data transformation work involves taking the budget and actuals files and joining additional details to them from reference files.

### Reference files

Using a Chart of Accounts, which is an Excel document containing multiple tabs, the script creates four reference files: Funds, Accounts, Departments, Projects. See more information about these reference files in SME notes below. We have also written a budget data guide for users available [here](https://data.sandiego.gov/budget-topic/). The reference files are joined to the budget data to add more details, such as the department funds center, funding repository, expense or revenue account description, or Capital Improvement Project description. Without this information, the budget data is extremely sparse and not usable for a citizen.

### Budget files

Capital budget data comes in two formats: Money budgeted (budget) or spent (actuals) in a single fiscal year and money budgeted or spent per project to date for that fiscal year. Operating budget data is per fiscal year only. Additionally, files may exist for the proposed budget versus the adopted budget. The script writes all of these files, but the links on the data portal are usually only updated to include the latest adopted budget.

Every time this script runs, it recreates the files for each fiscal year. This is because the chart of accounts may change. If it does, DOF re-extracts every fiscal year to match the latest chart of accounts.

Capital budget data has four original columns: amount, funds number, project number, and object number. The script performs 3 merges. The first is with funds reference where funds number (renamed code) = fund number. The second is with project reference where project number equals project number. The third is with accounts reference where object number equals account number.

Operating budget data and actuals data have four original columns: amount, funds number, departments/programs, and object number. For each file, the script performs 3 merges. The first is with funds reference where funds number (renamed code) = fund number. The second is with department reference where departments/programs (renamed dept_number) equals funds center number. The third is with accounts reference where object number (renamed commitment_item) equals account number.

Capital actuals has 5 original columns: amount, fund number, parent project, child project, and object number. The script performs the same 3 merges, but uses parent project to join to project reference. Child project is a newer addition to the dataset and is included because CIP project information from Public Works is available only at the child level for certain types of projects.

### Final field name map

#### Chart of Accounts Funds tab

| Columns in Excel | Columns in **funds ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Fund Type | fund_type |
| Funds | fund_name |
| (code) | fund_number |
| (group) | *dropped* |
| (ignored) | *dropped* |

#### Chart of Accounts Asset Owning Department tab

This is joined to Asset Type Project tab cols on (code)

| Columns in Excel | Columns in **projects ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Asset Owning Department | asset_owning_dept |
| Asset Owning Department Detail | *dropped* |
| (code) | project_number |
| (group) | *dropped* |
| (ignored) | *dropped* |
| notes | *dropped* |

#### Chart of Accounts Asset Type Project tab

This is joined to Asset Owning Department tab cols on (code)

| Columns in Excel | Columns in **projects ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Asset Type/Project | asset_type |
| Asset Type | asset_subtype |
| Asset Type Project | project_name |
| (code) | project_number |
| (group) | *dropped* |
| (ignored) | *dropped* |

#### Chart of Accounts Departments Programs tab

| Columns in Excel | Columns in **departments ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Department Group | dept_group |
| Department | dept_name |
| Division | dept_division |
| Section | dept_section |
| Fund Center | funds_center |
| Unspecified | *dropped* |
| (code) | funds_center_number |
| (group) | *dropped* |
| (ignored) | *dropped* |

#### Chart of Accounts Expenses tab

This is concatted to Revenues tab

| Columns in Excel | Columns in **accounts ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Expenses | *dropped* |
| Object Type | account_type |
| Object Class | account_class |
| Object Group | account_group |
| Object | account |
| (code) | account_number |
| (group) | *dropped* |
| (ignored) | *dropped* |

#### Chart of Accounts Revenues tab

This is concatted to Expenses tab

| Columns in Excel | Columns in **accounts ref** set |
| ------ | ------ |
| (key) | *dropped* |
| Revenues | *dropped* |
| Object Type | account_type |
| Object Class | account_class |
| Object Group | account_group |
| Object | account |
| (code) | account_number |
| (group) | *dropped* |
| (ignored) | *dropped* |

#### FYXX 2Date CIP Actuals

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Fund Number | fund_number |
| Parent Project | project_number_parent |
| Child Project | project_number_child |
| Object Number | account_number |

#### FYXX 2Date CIP Budget

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Fund Number | fund_number |
| Project Number | project_number |
| Object Number | account_number |
| Fiscal Year | *dropped. Should this be included or rolled up?* |

#### FYXX CIP Actuals

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Fund Number | fund_number |
| Parent Project | project_number_parent |
| Child Project | project_number_child |
| Object Number | account_number |

#### FYXX CIP Budget

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Funds Number | fund_number |
| Project Number | project_number |
| Object Number | account_number |

#### FYXX Operating Actuals

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Funds Number | fund_number |
| Departments/Programs | funds_center_number |
| Object Number | account_number |

#### FYXX Operating Budget

| Columns in Excel | Columns in public data |
| ------ | ------ |
| Amount | amount |
| Funds Number | fund_number |
| Departments/Programs | funds_center_number |
| Object Number | account_number |

## Notes from SME

- Funds includes information about the funding repository and have a unique identifer for the fund number. 
- Accounts include information about the expense or revenue account and have a unique identifier for account number. 
- Departments includes information about the department funds centers. Funds center are low-level organizational units within departments and have a unique identifier. 
- Projects include information about Capital Improvement Projects and have a unique identifier for project number. 