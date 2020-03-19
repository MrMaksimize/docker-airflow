## About this job

This job creates both sets of datasets containing development permits. It also creates subfiles that other departments use.

Data sources: PTS and Accela systems

Owning department: Development Services

SMEs: Thuy Le, TLe@sandiego.gov, and David Saborio, DSaborio@sandiego.gov

Date last updated: February 2020

## To Dos

- Figure out how to handle closed projects file
- As Accela takes on additional permit types, check back with department to find out what changes may have been made to the database schema.

## Data transformations

The source of the data is a set of 5 files dumped onto the DataSD FTP site every Sunday night from both DSD systems. The data is divided into files by source system (the two systems have a different schema) and then by approval status (closed or active). The first task loops through a dictionary containing information about each of the 5 files, looks for a version of each with the most recent date, and downloads to the temp folder.

### Creating PTS files

This set of tasks (contained in the create_files subdag) creates either the active set or the closed set, depending on mode. Most of the data transformation involves changing field names. There is one additional step of checking the approval ids contained in the active set against a file of approvals associated with closed projects. The final output is one file of active permits and one file of closed permits, which both go to temp. Eventually, PTS use will discontinue and all permits will be handled in Accela only.

### Creating Accela files

This set of tasks (contained in the create_files subdag) also creates either an active set or a closed set, depending on mode. Again, most of the data transformation involves changing field names, but it also removes fields that have all null values and strips extra whitespace. The final output is one file of active permits and one file of closed permits, and these also go to temp.

### Joining BIDs

This set of tasks (contained in the join_bids subdag) performs a spatial join of permits to Business Improvement District polygons for each of the 4 files and outputs a final version of the file to the prod folder.

### Creating a subset for TSW

Transportation and Storm Water Department includes permits issued for the Right-of-Way within a spatial conflict management application. We are supporting a system integration between DSD source systems and the conflict management application. The technical aspect of this integration is managed by GIS contractor Quartic. The business side of this integration is managed by TSW.

The task creates a subset of permits that are a certain approval type and have a status of Issued.

### Creating a subset for Public Works

Public Works inspection employees bill their time to DSD projects. We are creating a file that is used to populate a dropdown for employees to use when billing time in SAP. The business side of this integration is managed by Public Works, and the technical side is managed by the SAP team.

The task creates a subset of permits that are a certain approval type and have a status of either Issued or Complete. This is a large subset of permits and requires pulling records from historical PTS, current PTS and current Accela. A particular permit type in Accela (Traffic Control Plan-Permit) has less information than other permit types, so we perform some field mapping. The "ID" field is a mix of project id (everything else) and approval id (traffic permit from Accela). The "Title" field is a mix of project title (everything else) and a combination of approval type and address of job (traffic permit from Accela)

