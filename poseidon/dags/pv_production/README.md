## About this job

This job creates datasets containing energy produced by solar photovoltaic systems and also integrates with 3rd-party dashboards for grant compliance.

Data sources: PowerFactors API

Owning department: Sustainability
SMEs: Shannon Sales, SSalese@sandiego.gov, and Bryan Olson, OlsonB@sandiego.gov

Date last updated: May 2020

## To Dos

- Add functionality for trident.util.seaboard_updates.update_json_date

## Data transformations

The source of the data is a REST API with Power Factors, who have been contracted to provide the PV data. The data is queried from the API hourly, combined and saved to /data/temp, pushed to Lucid for dashboard integration, and appends a production dataset in /data/prod of all PV data since 2020. The production dataset will be uploaded to S3 once a day. 

### get_pv_data_write_temp

This function orchestrates calls to the Power Factor API, by utilizing two helper functions: API_to_csv and get_data. API_to_csv utilizes the get_data helper function to make calls, and stores the returned values to csv files in /data/temp. This function stores the information to make both hourly and daily calls. get_data is a function that creates the proper HTTP request for the API.

Daily calls pull 3 days worth of data. This overlap is necessary to ensure all interval data is included because the API may be unreliable in the data it returns. Daily data is appended to production file and deduplicated.

### update_pv_prod

This function orchestrates the joining of the new data from /data/temp into the production file in /data/prod by utilizing the build_production_files helper function, and replaces the production dataset in /data/prod. 