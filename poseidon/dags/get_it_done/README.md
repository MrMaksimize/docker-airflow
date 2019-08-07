## About this job

This job creates the dataset of Get it Done requests published at [https://data.sandiego.gov/datasets/get-it-done-311/](https://data.sandiego.gov/datasets/get-it-done-311/). It also creates several datasets filtered from the main GID data, including Potholes, Graffiti, Illegal Dumping, and 72-hour parking violations.

Data source: Salesforce

Owning department: Performance & Analytics

Contact person: Alex Hempton, AHempton@sandiego.gov

## To Dos

None

## Data transformations

The source of this data is a report created in Salesforce using the Data Portal Integration log in. The first task is to use a SOAP request to extract the contents of that report by report id. 

The report itself contains a few filters. First, it pulls records starting from 5/20/2016, which is the date the initial pilot was launched. Secondly, it only includes records with a Case Record Type equal to: 72 Hour Report, TSW ROW, Storm Water Code Enforcement, Parking, ESD Complaint/Report, DSD, Traffic Engineering Closed Case, Street Division Closed Case, Storm Water Closed Case, Traffic Engineering, Street Division, and Storm Water. Finally, it removes records that were added to the system as part of the data migration for ESD. These records were not requests submitted by GID users.

### Fix case record type for consistency

The variety of string values in the Case Record Type column in the source data become one of: Parking, TSW, Traffic Engineering, DSD, TSW ROW, ESD Complaint/Report, and Storm Water Code Enforcement.

### Create a service_name field

The goal of the creating the service name field is to provide data users with one column they can use to filter the data according to the familiar service names in the app. For example, users see "Parking issue" as an option in the app, but in the data, the users would have to look for "Abandoned Vehicle" in a Problem Category column as well as "72 Hour Report" in the Case Record Type column to find reports for "Parking issue."

The Salesforce database schema has changed as more service types have been added, so the script creates case_type_new and case_sub_type_new fields based off of the values in these 10 fields:

- Problem Category
- Case Type
- Parking Violation Type
- SAP Problem Category
- SAP Problem Type
- SAP Subject Category
- SAP Subject Type
- Problem Category Detail
- Violation Name
- Violation Type

These 10 fields are later dropped. The two new fields are used to join to an [external crosswalk file](https://datasd-reference.s3.amazonaws.com/gid/gid_crosswalk.csv). From that join, the data gets another new field, case_category, which is later renamed to service_name. The crosswalk was manually created by Alex Hempton, and it standardizes a large number of very granular descriptions in the case_type_new and case_sub_type_new columns.

### Correct closure dates

### Final field name map

| Columns in source | Columns in final data |
| ------ | ------ |
| *N/A* | council_district |
| *N/A* | comm_plan_code |
| *N/A* | comm_plan_name |
| *N/A* | park_name |
| *N/A* | service_name |
| Date/Time Opened | date_requested |
| Problem Category | *dropped* |
| Date/Time Closed | date_updated |
| Mobile/Web Status | status |
| Case Origin | case_origin |
| Case Record Type | case_record_type |
| Case Type | *dropped* |
| Specify the Issue | *dropped except for Illegal Dumping subset* |
| Parking Violation Type | *dropped* |
| SAP Notification Number | sap_notification_number |
| Parent Case Number | service_request_parent_id |
| Case Number | service_request_id |
| Geolocation (Latitude) | lat |
| Geolocation (Longitude) | lng |
| SAP Problem Category | *dropped* |
| SAP Problem Type | *dropped* |
| SAP Subject Category | *dropped* |
| SAP Subject Type | *dropped* |
| Referred Department | referred |
| Problem Group | *dropped* |
| Problem Category Detail | *dropped* |
| Violation Name | *dropped* |
| Violation Type | *dropped* |
| Age (Days) | case_age_days |
| Hide from Web | *dropped* |
| Public Description | public_description |

## Notes from SME


