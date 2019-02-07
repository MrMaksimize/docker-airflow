* Remove all Records where Work Order = UTLY and Empty Completion Date
* Remove all Records where Work Order = TSW and Empty Completion Date
* Remove all records where Activity is empty
* Remove all records where Activity is one of the following:
    * Data Entry
    * Mill & Pave
    * Structure Widening
    * Patching
* Set Type Accordingly:
    * Concrete:
        * Panel Replacement
        * PCC Reconstruction
    * Slurry:
        * Surface Treatment
        * Scrub Seal
        * Cape Seal
        * Central Mix
    * Overlay
        * AC Overlay
        * Mill & Pave
        * AC Reconstruction
        * AC Inlay
* Remove All Records over 5 Years Old from TODAY
* Remove All Records where Type = Slurry and are Older than 3 years from TODAY
* Set all jobs where there is a completion date to moratorium status
* Set all records where Work Order = TSW and Completion Date in the future Status = Construction
* Remove jobs where:
    * Status is missing
    * Project Type is missing
    * Activity is missing
* Remove duplicate street segments, keeping the latest by completion date
* Remove Start and End Dates for projects that are in Moratorium

