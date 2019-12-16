## About this job

This folder contains three dags: Police calls for service, traffic collisions, and hate crimes. These three dags create the datasets published at [https://data.sandiego.gov/datasets/police-calls-for-service/](https://data.sandiego.gov/datasets/police-calls-for-service/), [https://data.sandiego.gov/datasets/police-collisions/](https://data.sandiego.gov/datasets/police-collisions/), and [https://data.sandiego.gov/datasets/police-hate-crimes/](https://data.sandiego.gov/datasets/police-hate-crimes/).

Calls for service and traffic collisions are updated daily, and hate crimes is triggered manually whenever PD supplies new data.

The data source for all PD data is the datasd FTP site. We cannot directly query the Police Department's data sources, so they either script nightly exports (calls for service and collisions) or manually upload (hate crimes and RIPA).

## To Dos

Add RIPA as a manually triggered dag. Standardize methods across dags.

## Data transformations

Transformations are simple because PD puts the data in our desired format at export.

### Calls for service

This job first hits the datasd folder containing nightly calls for service exports (sdpd/calls_for_service). Using a wildcard pattern, it downloads all daily files for the current year to the temp data folder.

Next, all of the files for the current year that are in the temp folder, including those that were previously downloaded, are loaded as dataframes and combined into one temp frame. A few of the columns in the temp frame that need to be string dtype are converted, then the temp frame is appended to the production file for that year, which is loaded from the prod data folder. Because we are loading in the entire year's files every time, we then need to deduplicate on incident ID.

### Collisions

This job also starts by hitting the datasd folder containing nightly collisions exports (sdpd/collisions). However, this job uses a different approach to download the raw files and uses a proprietary time flag to get only the latest file. That file is downloaded as temp_collisions.csv

In the next task, the temp collisions file in the temp data folder is appended to the production file, which is loaded from the prod data folder.

### Hate Crimes

A new Excel workbook containing hate crimes is provided by the department every other month. Each time a new workbook is provided, the workbook contains all data that needs to be published, so it completely overwrites existing data.

First, any new Excel files are downloaded from the datasd folder containing the department's manual uploads (sdpd/Hate_Crimes). The file with the latest date updated is used in the subsequent task to create the production file.

