## About this job

This job creates two water tests datasets: 
Monitoring of chemical analytes in the water distribution system (https://data.sandiego.gov/datasets/monitoring-of-select-chemical-parameters-in-drinking-water/) and Monitoring of chemical analytes in treatment plant effluents.

Data source: WPL

Owning department: Public Utilities Department EMTS division

SME: Keith Ruehrwein, kruehrwein@sandiego.gov

Date last updated: Tue Nov 24 2020

## To Dos

Pull historical data

## Data transformations

The source of this data is the same source system as the dataset for microbiological parameters. There are two SQL queries - one that extracts certain analytes for the drinking water distribution system, and one that extracts all analytes for water treatment plant effluents. After the two different SQL queries run, the transformations that happen to each file are nearly identical, so there is one function that is called in a subdag.

The two notable transformations are described below.

### Calculate mean for multiple analyte tests from the same sample

Some analytes are tested multiple times from the same sample, which is indicated by the test number field. These are aggregated into a mean result so that each analyte has one test per sample. 

### Calculate Nitrate as N

All tests for Nitrate are converted into Nitrate as N and added to the dataset.

