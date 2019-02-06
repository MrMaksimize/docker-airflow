City docs ETL pulls documents from 3 different Microsoft databases (Sire, OnBase, and Documentum) and save them as .csv files on production folder.
There are 3 different tables on Sire and OnBase databases
 1. council_dockets
 2. council_minutes
 3. council_results 

There are 55 different tables on Documentum database
 1. 24 tables with daily schedule to update and 
 2. 31 tables with one hour schedule to update so there 
    are two DAGs (documentum_24 and documentum_others) to accommodate the different schedules.


