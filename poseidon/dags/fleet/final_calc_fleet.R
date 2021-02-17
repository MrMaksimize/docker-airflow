#================================================================   
## Project name: ROC Fleet metrics
## Author: Zaira Razu, Data and Analytics, PandA
##Last edit: Nov 13th, 2020
#================================================================   
#Clear workspace
rm(list = ls())


library(data.table)
library(plyr)
library(dplyr)
library(plotly)
library(readr)
library(tidyr)
library(zoo)
library(stringr)
# library(lubridate) The following objects are masked from ‘package:data.table’:
# hour, isoweek, mday, minute, month, quarter, second, wday, week, yday, year



#================================================================   
## Define directories
#================================================================   

setwd("~/Code/roc-metrics/fleet_metrics")



#====================Load datasets=============================== 

vehicles<-read.csv("/data/prod/fleet_vehicles.csv")
work<-read.csv("/data/prod/fleet/fleet_jobs.csv")
delays<-read.csv("/data/prod/fleet/fleet_delays.csv")
dept_look<-read.csv("/data/prod/fleet/fleet-dept-lookup.csv") 
workdelay_look<-read.csv("/data/prod/fleet/fleet-workdelay-lookup.csv")



#=========== ID's for vehicles and for work orders

###Vehicles dataset
vehicles<- vehicles %>% mutate_all(na_if,"") #turn blanks into NA
length(unique(vehicles$equip_id))
#[1] 14262 #no duplicate id's

# Work orders dataset
work<-subset(work, date_opened> "2018-01-01") #keep only orders opened from 2018 onward
work<-subset(work, wo_status!="PENDING") 
work<-subset(work, work_loc!="FI" & work_loc!="MPT" & work_loc!= "FM") #remove work locations that do not affect availability


##concatenate work$wo_id and work$equip_id
work$woequip_id<-paste0(work$wo_id, "_", work$equip_id )
table(duplicated(work$woequip_id)) ##there are still some duplicates.
#  FALSE   TRUE 
# 134345    638 

##concatenate with date opened too
work$woequip_id_do<-paste0(work$woequip_id,"_", as.character(as.Date(work$date_opened)))
table(duplicated(work$woequip_id_do)) #Only 8 duplicates



##########################################################
#---------PREPARE DATA FOR METRIC CALCULATION---------------
##########################################################

##########################################################
#---------Vehicles Prep---------------
##########################################################


##-----------Clean VEHICLE data previous to merge with work order data----
#filter vehicles by asset type 
vehicles_original<-vehicles #save backup original version before transformations
vehicles<-subset(vehicles, asset_type=="ASSET") 

vehicles$date_retired<-as.Date(vehicles$date_retired)
vehicles$date_added<-as.Date(vehicles$date_added)


vehicles<-subset(vehicles, date_added<date_retired |is.na(date_retired)==TRUE) #keep only vehicles with date added smaller than date retired (deleting possible system errors)
#around 10% of vehicles removed (removing 1272, keeping 12668)


##I will not filter by proc_status because that would remove all  **currently** retired vehicles 
## and will not allow for a historic analysis. 

## Create column subset of valid vehicles including relevant characteristics, to join with work, department, and type dataframes
valid_veh<-select(vehicles, equip_id, priority_code, dept_code,description, stds_class,acat_cat_no, year, date_added, date_retired)


#Recategorize Priority Code

valid_veh$priority_recode<-ifelse(valid_veh$priority_code=="1"|
                                    valid_veh$priority_code=="1A"|
                                    valid_veh$priority_code=="1B"|
                                    valid_veh$priority_code=="1C"|
                                    valid_veh$priority_code== "1D"|
                                    valid_veh$priority_code== "1E"|
                                    valid_veh$priority_code== "1F",
                                  1, as.character(valid_veh$priority_code))



##----------Subset and clean DELAYS data for AVAILABILITY CALCULATIONS
#Only interested in delays that meant the vehicle was still available even if a work order was open
down_delay<-subset(delays, work_delay_code=="S" | work_delay_code=="ST")
down_delay<-droplevels(down_delay) #drop unused categories
down_delay$date_delay_start<-as.Date(down_delay$date_delay_start)
down_delay$date_delay_end<-as.Date(down_delay$date_delay_end)

#remove rows where date_delay_start>date_delay_end
down_delay<-subset(down_delay, date_delay_start<=date_delay_end)
down_delay<-subset(down_delay, delay_hours>0)

#remove work_loc LU and RD (per their SQL query)
down_delay<-subset(down_delay, work_loc!="LU" & work_loc!="RD")


######################################################
##-----------Department classification-  ##-----------
######################################################

valid_veh$valid_veh<-1 #create indicator variable to quickly evaluate merge with department lookup
valid_veh<- left_join(valid_veh, dept_look) ###join to department lookup table (automatically uses dept_code to join)
table(valid_veh$valid_veh) #all rows merged succesfully
valid_veh$valid_veh<-NULL #remove unnecessary column

valid_veh$dept_group<-ifelse(grepl("ENVIRONMENTAL",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"ESD", "Other") 
valid_veh$dept_group<-ifelse(grepl("ES/",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"ESD", valid_veh$dept_group) 
valid_veh$dept_group<-ifelse(grepl("TRANSPORTATION",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"TSW", valid_veh$dept_group) 
valid_veh$dept_group<-ifelse(grepl("PUBLIC",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"PUD", valid_veh$dept_group)
valid_veh$dept_group<-ifelse(grepl("PUD/",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"PUD", valid_veh$dept_group) 
valid_veh$dept_group<-ifelse(grepl("PARK",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"Parks", valid_veh$dept_group) 
valid_veh$dept_group<-ifelse(grepl("FIRE",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"Fire", valid_veh$dept_group) 
valid_veh$dept_group<-ifelse(grepl("POLICE",valid_veh$dept_name, ignore.case = TRUE)==TRUE,"Police", valid_veh$dept_group) 




######################################################################
##----------------Vehicle classification and tables by department ----
######################################################################

###Create vehicle categories
valid_veh$veh_group<-ifelse(grepl("TRAILER",valid_veh$description, ignore.case = TRUE)==TRUE,"TRAILER", "Other") 
valid_veh$veh_group<-ifelse(grepl("SUV",valid_veh$description, ignore.case = TRUE)==TRUE,"SUV_nonpat", valid_veh$veh_group)
valid_veh$veh_group<-ifelse(grepl("SEDAN",valid_veh$description, ignore.case = TRUE)==TRUE,"SEDAN_nonpat", valid_veh$veh_group)
valid_veh$veh_group<-ifelse(grepl("SDN",valid_veh$description, ignore.case = TRUE)==TRUE,"SEDAN_nonpat", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("PATROL",valid_veh$description, ignore.case = TRUE)==TRUE,"Patrol", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("PU-",valid_veh$description, ignore.case = TRUE)==TRUE,"PU", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("PU ",valid_veh$description, ignore.case = TRUE)==TRUE,"PU", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("REFUSE",valid_veh$description, ignore.case = TRUE)==TRUE,"PACKER-REFUSE", valid_veh$veh_group)
valid_veh$veh_group<-ifelse(grepl("PACKER",valid_veh$description, ignore.case = TRUE)==TRUE,"PACKER-REFUSE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("VAN",valid_veh$description, ignore.case = TRUE)==TRUE,"VAN", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("SCOOTER",valid_veh$description, ignore.case = TRUE)==TRUE,"SCOOTER", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("TRACTOR",valid_veh$description, ignore.case = TRUE)==TRUE,"TRACTOR", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("TRK",valid_veh$description, ignore.case = TRUE)==TRUE,"TRUCK", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("TRUCK",valid_veh$description, ignore.case = TRUE)==TRUE,"TRUCK", valid_veh$veh_group) #There are a few cases that are truck and van or tractor at the same time, i will categorize as truck.
valid_veh$veh_group<-ifelse(grepl("COMPRESS",valid_veh$description, ignore.case = TRUE)==TRUE,"TRUCK", valid_veh$veh_group) #most compressors have the word "truck" on them, but not all do. i will classify all of them as truck.
valid_veh$veh_group<-ifelse(grepl("MOTORCYCLE",valid_veh$description, ignore.case = TRUE)==TRUE,"MOTORCYCLE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("VACTOR",valid_veh$description, ignore.case = TRUE)==TRUE,"VACTOR", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("SWEEPER",valid_veh$description, ignore.case = TRUE)==TRUE,"SWEEPER", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("COUPE",valid_veh$description, ignore.case = TRUE)==TRUE,"COUPE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("FORKLIFT",valid_veh$description, ignore.case = TRUE)==TRUE,"FORKLIFT", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("ENGINE",valid_veh$description, ignore.case = TRUE)==TRUE,"ENGINE/RESCUE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("TYPE-1-500",valid_veh$description, ignore.case = TRUE)==TRUE,"ENGINE/RESCUE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("AERIAL",valid_veh$description, ignore.case = TRUE)==TRUE,"ENGINE/RESCUE", valid_veh$veh_group)
valid_veh$veh_group<-ifelse(grepl("RESCUE",valid_veh$description, ignore.case = TRUE)==TRUE,"ENGINE/RESCUE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("RESPONSE",valid_veh$description, ignore.case = TRUE)==TRUE,"ENGINE/RESCUE", valid_veh$veh_group) 
valid_veh$veh_group<-ifelse(grepl("ELECTRIC",valid_veh$description, ignore.case = TRUE)==TRUE,"ELECTRIC", valid_veh$veh_group)
valid_veh$veh_group<-ifelse(grepl("CART",valid_veh$description, ignore.case = TRUE)==TRUE,"ELECTRIC", valid_veh$veh_group)

##Create alternative grouping using stds_class first 2 digits
valid_veh$std_group<- substr(valid_veh$stds_class, start = 1, stop = 2)
table(valid_veh$std_group)
std_group_t<- data.frame(table(valid_veh$std_group)) 


##save working dataset
write_csv (valid_veh, "valid_vehicles.csv")

######################################################################
##----------------Work Orders Prep ----
######################################################################
work_original<-work #save backup original version before transformations
nrow(work) #135987 obs. of  21 variables
work<-subset(work, work_loc!= "LU" & work_loc!= "RD") #keep only work orders from not LU and not RD work_loc
nrow(work) #125371 obs. of  21 variables

work$dateT_opened<-work$date_opened #save both date and time on this field
work$dateT_finished<-work$date_finished #save both date and time on this field
work$date_finished<-as.Date(work$date_finished) #assign date format (drops time)
work$date_opened<-as.Date(work$date_opened) #assign date format (drops time)

### I will use date opened based on my understanding of David's description. If this is wrong I can change for unit in.
work<-subset(work, is.na(date_opened)==FALSE) #keep only rows with date_opened

##Remove those weird cases (.3% of work orders that were finished before they were opened) but without removing NA finish dates
work<-subset(work, (date_opened<=date_finished | is.na(date_finished)==TRUE)) #BUT keep those that were opened on the same date they were finished, as some work orders take less than a full day.
nrow(work) # [1] 125134

####Join to "valid vehicle" data by equip_id
valid_veh$valid<-1
work_veh<-left_join(work, valid_veh, by= "equip_id")
work_veh<-subset(work_veh, is.na(valid)==FALSE) #Remove work orders from non valid vehicles
work_veh$valid<-NULL #Remove unnecessary column
nrow(work_veh) #[1] 124136

work_veh<-subset(work_veh, date_opened<as.Date(date_retired) | is.na(date_retired)==TRUE) #keep only work orders that started before vehicle was retired
nrow(work_veh)#[1] 123601


work_veh<-subset(work_veh, date_opened>=as.Date(date_added)) #keep only work orders that started after or on the same date vehicle was added
nrow(work_veh) #[1] 123601

#concatenate work id and equip id
work_veh$woequip_id<-paste0(work_veh$wo_id, "_", work_veh$equip_id )
table(duplicated(work_veh$woequip_id))
#concatenate to date open
work_veh$woequip_id_do<-paste0(work_veh$woequip_id,"_", as.character(as.Date(work_veh$date_opened)))
table(duplicated(work_veh$woequip_id_do)) ##no duplicates

##Work order calculated fields
work_veh$time_finish<- as.numeric(work_veh$date_finished-work_veh$date_opened)
work_veh$time_finish_h<-as.numeric(difftime(work_veh$dateT_finished, work_veh$dateT_opened, units = "hours")) 
#some work orders have negative hours because the time of finish is on the same day but at a later time we will assume these are data entry errors
error<-ifelse(work_veh$time_finish_h<0,1,0)
#only one percent of work orders have this issue, will turn calculated field to na
work_veh$time_finish_h<-ifelse(work_veh$time_finish_h<0,NA,work_veh$time_finish_h)

##there are also some negative values on labor hours, we will assume for now that those are errors (20 cases only)
error<-ifelse(work_veh$labor_hours<0,1,0)
work_veh$labor_hours<-ifelse(work_veh$labor_hours<0,NA,work_veh$labor_hours)

##same thing for labor costs, parts costs, and total costs
work_veh$labor_cost<-ifelse(work_veh$labor_cost<0,NA,work_veh$labor_cost)
work_veh$parts_cost<-ifelse(work_veh$parts_cost<0,NA,work_veh$parts_cost)
work_veh$total_cost<-ifelse(work_veh$total_cost<0,NA,work_veh$total_cost)

##labor time as a proportion of time to finish an order
work_veh$share_labor_time<-work_veh$labor_hours/work_veh$time_finish_h
work_veh$share_labor_time<- ifelse(is.na(work_veh$time_finish_h)==TRUE, NA, work_veh$share_labor_time)
work_veh$share_labor_time<- ifelse(work_veh$time_finish_h==0, NA, work_veh$share_labor_time)
summary(work_veh$share_labor_time)
#     Min.  1st Qu.   Median     Mean  3rd Qu.     Max.     NA's 
#   0.0000   0.0417   0.1739   0.6071   0.4778 252.0000     2342 
##the average proportion is higher than 1 (more labor hours worked than hours of time to finish) 
## meaning that at least two people were working on the order- also extremely high for cases where the order was finished (not necessarily closed) 
##in less than an hour and yet has 8+ hours of labor

##fields to aggregate data
work_veh$year_mon_o<- format(as.Date(work_veh$date_opened), "%Y-%m")

#save work orders working dataset
write_csv(work_veh, "valid_veh_wo.csv")

########  ######## ########   ######## #####  ######## ##### ######## ##### 
######## METRIC- Availability Calculations by priority code  ######## ##### 
########  ######## ########  ######## #####  ######## ##### ######## ##### 

############---------------------
###total stock by day by pcode- nested loop
######----------------------------
valid_veh<-data.table(valid_veh)
pcodes <- c(1,2) ### priority code values to loop through

##Create empty dataframe to fill in with calculated fields
vehicles_daily_pcode<-NULL

for (val in pcodes) #outside loop: through priority codes
  
{
  p_vehicles<-valid_veh[priority_recode==val,]
  ##2015-2020
  dates <- seq(as.Date("2018-07-01"), as.Date(Sys.Date()), by=1) #dates to loop over; 
  
  ##Create empty dataframe to fill in with calculated fields
  vehicles_daily_p<-NULL
  
  
  for (i in seq_along(dates)) #inside loop: through each day of every year
  { 
    # cal_date <- dates[i]
    ##Vehicles available by day
    p_vehicles<-valid_veh[priority_recode==val,]
    #p_vehicles[date_added <= cal_date & date_retired > cal_date, vehicle_present:= 1]
    p_vehicles[, vehicle_present:= ifelse((dates[i]>=as.Date(date_added) & dates[i]<as.Date(date_retired))|
                                            (dates[i]>=as.Date(date_added) & is.na(date_retired)==TRUE),1, 0)]
    p_vehicles<-p_vehicles[vehicle_present==1,]
    
    ##Fill in empty matrix with calculated daily
    vehicles_daily_p<-rbind(vehicles_daily_p, data.frame( "date"= dates[i],
                                                          "pcode"=val,
                                                          "stock"= length(unique(p_vehicles$equip_id,na.rm=TRUE)),
                                                          "s_ELECTRIC"= sum(p_vehicles$veh_group== "ELECTRIC", na.rm=TRUE),
                                                          "s_ENGINE/RESCUE"= sum(p_vehicles$veh_group== "ENGINE/RESCUE", na.rm=TRUE),
                                                          "s_MOTORCYCLE"= sum(p_vehicles$veh_group== "MOTORCYCLE", na.rm=TRUE),
                                                          "s_PACKER-REFUSE"= sum(p_vehicles$veh_group== "PACKER-REFUSE", na.rm=TRUE),
                                                          "s_VACTOR"=sum(p_vehicles$veh_group=="VACTOR", na.rm=TRUE),
                                                          "s_PATROL"= sum(p_vehicles$veh_group== "Patrol", na.rm=TRUE),
                                                          "s_PU"= sum(p_vehicles$veh_group== "PU", na.rm=TRUE),
                                                          "s_SCOOTER"= sum(p_vehicles$veh_group== "SCOOTER", na.rm=TRUE),
                                                          "s_SEDAN_nonpat"= sum(p_vehicles$veh_group== "SEDAN_nonpat", na.rm=TRUE),
                                                          "s_SUV_nonpat"= sum(p_vehicles$veh_group== "SUV_nonpat", na.rm=TRUE),
                                                          "s_TRACTOR"= sum(p_vehicles$veh_group== "TRACTOR", na.rm=TRUE),
                                                          "s_TRAILER"= sum(p_vehicles$veh_group== "TRAILER", na.rm=TRUE),
                                                          "s_TRUCK"= sum(p_vehicles$veh_group== "TRUCK", na.rm=TRUE),
                                                          "s_VAN"= sum(p_vehicles$veh_group== "VAN", na.rm=TRUE),
                                                          "s_SWEEPER"=sum(p_vehicles$veh_group=="SWEEPER", na.rm=TRUE),
                                                          "s_FORKLIFT"=sum(p_vehicles$veh_group=="FORKLIFT", na.rm=TRUE),
                                                          "s_COUPE"=sum(p_vehicles$veh_group=="COUPE", na.rm=TRUE),
                                                          "s_OTHER"= sum(p_vehicles$veh_group== "Other", na.rm=TRUE),
                                                          "mean_age"= mean(year(dates[i])-p_vehicles$year),
                                                          "median_age"= median(year(dates[i])-p_vehicles$year)
    ) 
    #Sum all vehicles that had been added by date "i" and not yet retired by date "i"
    )
  }
  
  vehicles_daily_pcode<-rbind(vehicles_daily_pcode,vehicles_daily_p)
}

# ##quality check using 2020-06-30 
# test<-valid_veh[as.Date("2020-06-30")>=as.Date(date_added) & (as.Date("2020-06-30")<as.Date(date_retired)|  is.na(date_retired)==TRUE),] #it matches the loop calc.
# table(test$priority_recode)
# 
# test2<-valid_veh[as.Date("2020-07-30")>=as.Date(date_added) & (as.Date("2020-07-30")<as.Date(date_retired)|  is.na(date_retired)==TRUE),] #it matches the loop calc.
# table(test2$priority_recode)






############---------------------
###vehicles being repaired by day by pcode
######----------------------------
work_veh<-data.table(work_veh) #declare data table type
pcodes <- c(1,2) ### priority code values to loop through

##Create empty dataframe to fill in with calculated fields
repairs_daily_pcode<-NULL

for (val in pcodes) #outside loop: through priority codes
  
{
  p_work_veh<-subset(work_veh, priority_recode==val)
  
  repairs_daily_p<-NULL #create empty data frame to fill in with calculated fields
  
  for (i in seq_along(dates)) { 
    
    ##Vehicles repaired by day
    p_work_veh<-subset(work_veh, priority_recode==val)
    p_work_veh[, vehicle_work:= ifelse((dates[i]>=as.Date(date_opened) & dates[i]<as.Date(date_finished)) |
                                         (dates[i]>=as.Date(date_opened) & is.na(date_finished)==TRUE),1, 0)] #all orders open
    p_work_veh[, new_order:= ifelse(dates[i]==as.Date(date_opened),1, 0)] #new orders
    
    
    date_work_veh<-subset(p_work_veh, vehicle_work==1) #subset of rows where for date "i" there is work being done on a vehicle
    date_work_veh<-date_work_veh[!duplicated(date_work_veh$equip_id), ] #keep only one row per vehicle, per day
    
    date_work_new_veh<-subset(p_work_veh, new_order==1) #subset of rows where for date "i" there is a new work order
    date_work_new_veh<-date_work_new_veh[!duplicated(date_work_new_veh$equip_id), ] #keep only one row per vehicle, per day
    
    
    
    ##Fill in empty matrix with calculated daily
    repairs_daily_p<-rbind(repairs_daily_p, data.frame( "date"= dates[i],
                                                        "pcode"=val,
                                                        "work"= uniqueN(date_work_veh$equip_id, na.rm=TRUE),
                                                        "r_ELECTRIC"= sum(date_work_veh$veh_group== "ELECTRIC", na.rm=TRUE),
                                                        "r_ENGINE/RESCUE"= sum(date_work_veh$veh_group== "ENGINE/RESCUE", na.rm=TRUE),                                                        
                                                        "r_MOTORCYCLE"= sum(date_work_veh$veh_group== "MOTORCYCLE", na.rm=TRUE),
                                                        "r_PACKER-REFUSE"= sum(date_work_veh$veh_group== "PACKER-REFUSE", na.rm=TRUE),
                                                        "r_VACTOR"=sum(date_work_veh$veh_group=="VACTOR", na.rm=TRUE),
                                                        "r_PATROL"= sum(date_work_veh$veh_group== "Patrol", na.rm=TRUE),
                                                        "r_PU"= sum(date_work_veh$veh_group== "PU", na.rm=TRUE),
                                                        "r_SCOOTER"= sum(date_work_veh$veh_group== "SCOOTER", na.rm=TRUE),
                                                        "r_SEDAN_nonpat"= sum(date_work_veh$veh_group== "SEDAN_nonpat", na.rm=TRUE),
                                                        "r_SUV_nonpat"= sum(date_work_veh$veh_group== "SUV_nonpat", na.rm=TRUE),
                                                        "r_TRACTOR"= sum(date_work_veh$veh_group== "TRACTOR", na.rm=TRUE),
                                                        "r_TRAILER"= sum(date_work_veh$veh_group== "TRAILER", na.rm=TRUE),
                                                        "r_TRUCK"= sum(date_work_veh$veh_group== "TRUCK", na.rm=TRUE),
                                                        "r_VAN"= sum(date_work_veh$veh_group== "VAN", na.rm=TRUE),
                                                        "r_SWEEPER"=sum(date_work_veh$veh_group=="SWEEPER", na.rm=TRUE),
                                                        "r_FORKLIFT"=sum(date_work_veh$veh_group=="FORKLIFT", na.rm=TRUE),
                                                        "r_COUPE"=sum(date_work_veh$veh_group=="COUPE", na.rm=TRUE),
                                                        "r_OTHER"= sum(date_work_veh$veh_group== "Other", na.rm=TRUE),
                                                        "new_work_orders"= uniqueN(date_work_new_veh$woequip_id_do, na.rm=TRUE),
                                                        "average_time_finish"= mean(date_work_new_veh$time_finish, na.rm=TRUE),
                                                        "median_time_finish"= median(date_work_new_veh$time_finish, na.rm=TRUE),
                                                        "Repairs"= sum(date_work_veh$job_type== "REPAIR", na.rm=TRUE),
                                                        "PM"= sum(date_work_veh$job_type== "PM", na.rm=TRUE))
                           #count number of unique vehicles vehicles being worked on date "i")
    )
    
  }
  
  repairs_daily_pcode<-rbind(repairs_daily_pcode,repairs_daily_p)
  
}

##quality check
#test<-work_veh[priority_recode==1]
#test<-test[as.Date("2020-06-30")>=as.Date(date_opened) & as.Date("2020-06-30")<as.Date(date_finished)|
             #("2020-06-30">=as.Date(date_opened) & is.na(date_finished)==TRUE),] 
#test<-test[!duplicated(test$equip_id), ]#keep only one row per vehicle, per day
#uniqueN(test$equip_id) #it matches the loop calc.
#table(test$veh_group)


############------------------------
###vehicles with open work orders but still available (delayed repair) by pcode 
######----------------------------
#down_delay$woequip_id<-paste0(down_delay$wo_id, "_", down_delay$equip_id )
###In the new version the name of the variable for work order number changed on the delays dataset, so:
down_delay$woequip_id<-paste0(down_delay$work_order_no, "_", down_delay$equip_id )
table(duplicated(down_delay$woequip_id)) ##there are still some duplicates, but it won't affect the metric as I will count unique vehicles on delay by date
work_veh$valid_wo<-1
down_delay<-left_join(down_delay, work_veh, by= "woequip_id")
table(is.na(down_delay$valid_wo)) #around 3% of delays don't have a match on work order data, remove those
down_delay<-subset(down_delay, is.na(valid_wo)==FALSE)
down_delay<- subset(down_delay, down_delay$date_delay_start>=down_delay$date_opened )
down_delay<- subset(down_delay, down_delay$date_delay_end<=down_delay$date_finished)
##Reduce dataframe
down_delay<-select(down_delay, equip_id.x, date_delay_start, date_delay_end, work_delay_code, woequip_id_do, priority_recode, dept_group, veh_group)
names(down_delay)[names(down_delay) == "equip_id.x"] <- "equip_id"

down_delay<-data.table(down_delay) #declare data table type


pcodes <- c(1,2) ### priority code values to loop through

##Create empty dataframe to fill in with calculated fields
still_used_daily_pcode<-NULL


for (val in pcodes) #outside loop: through priority codes
  
{
  p_down_delay<-subset(down_delay, priority_recode==val)
  
  still_used_p<-NULL #create empty data frame to fill in with calculated fields
  
  for (i in seq_along(dates)) {
    
    p_down_delay<-subset(down_delay, priority_recode==val)
    
    ##Vehicles with work order but still available (being used) by day
    p_down_delay[, still_used:= ifelse(dates[i]>=as.Date(date_delay_start) & dates[i]<as.Date(date_delay_end),1, 0)]
    still_used<-subset(p_down_delay, still_used==1) #subset of rows where for date "i" vehicle is still being used in spite of open work order
    still_used<-still_used %>% distinct(equip_id, .keep_all = TRUE) #keep only one row per vehicle
    
    
    ##Fill in empty matrix with calculated daily
    still_used_p<-rbind(still_used_p, data.frame( "date"= dates[i],
                                                  "pcode"=val,
                                                  "in_use"= uniqueN(still_used$equip_id,na.rm=TRUE),
                                                  "su_ELECTRIC"= sum(still_used$veh_group== "ELECTRIC", na.rm=TRUE),
                                                  "su_ENGINE/RESCUE"= sum(still_used$veh_group== "ENGINE/RESCUE", na.rm=TRUE),                                                
                                                  "su_MOTORCYCLE"= sum(still_used$veh_group== "MOTORCYCLE", na.rm=TRUE),
                                                  "su_PACKER-REFUSE"= sum(still_used$veh_group== "PACKER-REFUSE", na.rm=TRUE),
                                                  "su_Vactor"=sum(still_used$veh_group=="VACTOR", na.rm=TRUE),
                                                  "su_Patrol"= sum(still_used$veh_group== "Patrol", na.rm=TRUE),
                                                  "su_PU"= sum(still_used$veh_group== "PU", na.rm=TRUE),
                                                  "su_SCOOTER"= sum(still_used$veh_group== "SCOOTER", na.rm=TRUE),
                                                  "su_SEDAN_nonpat"= sum(still_used$veh_group== "SEDAN_nonpat", na.rm=TRUE),
                                                  "su_SUV_nonpat"= sum(still_used$veh_group== "SUV_nonpat", na.rm=TRUE),
                                                  "su_TRACTOR"= sum(still_used$veh_group== "TRACTOR", na.rm=TRUE),
                                                  "su_TRAILER"= sum(still_used$veh_group== "TRAILER", na.rm=TRUE),
                                                  "su_TRUCK"= sum(still_used$veh_group== "TRUCK", na.rm=TRUE),
                                                  "su_VAN"= sum(still_used$veh_group== "VAN", na.rm=TRUE),
                                                  "su_SWEEPER"=sum(still_used$veh_group=="SWEEPER", na.rm=TRUE),
                                                  "su_FORKLIFT"=sum(still_used$veh_group=="FORKLIFT", na.rm=TRUE),
                                                  "su_COUPE"=sum(still_used$veh_group=="COUPE", na.rm=TRUE),
                                                  "su_OTHER"= sum(still_used$veh_group== "Other", na.rm=TRUE)
    )
    #count number of unique vehicles vehicles with open work orders but still being used on date "i"
    )
  }
  
  still_used_daily_pcode<-rbind(still_used_daily_pcode,still_used_p)
  
}


##quality check
#test<-down_delay[priority_recode==1]

#test<-test[as.Date("2020-06-30")>=as.Date(date_delay_start) & as.Date("2020-06-30")<as.Date(date_delay_end),] 
#test<-test[!duplicated(test$equip_id), ]#keep only one row per vehicle, per day
#uniqueN(test$equip_id) #it matches the loop calc.
#table(test$veh_group)




###PCODE METRIC CALCULATIONS
#order dataframes
vehicles_daily_pcode<-vehicles_daily_pcode[with(vehicles_daily_pcode, order(pcode,date)), ]
repairs_daily_pcode<-repairs_daily_pcode[with(repairs_daily_pcode, order(pcode,date)), ]
still_used_daily_pcode<-still_used_daily_pcode[with(still_used_daily_pcode, order(pcode,date)), ]

available_pcode<-vehicles_daily_pcode[,-c(1,2, 22,23)] -repairs_daily_pcode[,-c(1,2, 22, 23, 24, 25, 26)] + still_used_daily_pcode[, -c(1,2)]
percent_available_pcode<-available_pcode/ vehicles_daily_pcode[,-c(1,2, 22,23)]


colnames(available_pcode)<-paste("av", colnames(available_pcode), sep="_") #change col names
colnames(percent_available_pcode)<-paste("pc", colnames(percent_available_pcode), sep="_") #change col names
idrow<-select(vehicles_daily_pcode, date, pcode)
available_pcode<-cbind(idrow, available_pcode)
percent_available_pcode<-cbind(idrow, percent_available_pcode)

###join all dataframes and analyze by pcode
pcode_metrics<-join_all(list(vehicles_daily_pcode, repairs_daily_pcode, still_used_daily_pcode, available_pcode, percent_available_pcode), by=c('date', 'pcode'), type='left')


tapply(pcode_metrics$pc_stock, pcode_metrics$pcode, summary)
# $`1`
#   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.8251  0.8869  0.9016  0.8968  0.9138  0.9443 
# 
# $`2`
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.9083  0.9256  0.9366  0.9362  0.9446  0.9613 


pcode_metrics_1<-subset(pcode_metrics, pcode==1)
pcode_metrics_2<-subset(pcode_metrics, pcode==2)

pcode_metrics_1<-pcode_metrics_1 %>%
  select(date, starts_with("pc"))
pcode_metrics_1$pcode<-NULL

pcode_metrics_2<-pcode_metrics_2 %>%
  select(date, starts_with("pc"))
pcode_metrics_2$pcode<-NULL

pcode_metrics_w<-left_join(pcode_metrics_1, pcode_metrics_2, by="date")
write_csv(pcode_metrics_w, "pcode_metrics_w.csv")

pcode_metrics_L<-pcode_metrics %>% 
  select(date, starts_with("pc"))
write_csv(pcode_metrics_L, "pcode_metrics_L.csv")




by_pcode_veh_pc<-as.data.frame(aggregate(pcode_metrics[,which(colnames(pcode_metrics)=="pc_stock"):which(colnames(pcode_metrics)=="pc_s_OTHER")],by=list(pcode_metrics$pcode), mean))
by_pcode_veh_ava<-as.data.frame(aggregate(pcode_metrics[,which(colnames(pcode_metrics)=="av_stock"):which(colnames(pcode_metrics)=="av_s_OTHER")],by=list(pcode_metrics$pcode), mean))
by_pcode_veh_stock<-as.data.frame(aggregate(pcode_metrics[,which(colnames(pcode_metrics)=="stock"):which(colnames(pcode_metrics)=="median_age")],by=list(pcode_metrics$pcode), mean))
by_pcode_veh_repair<-as.data.frame(aggregate(pcode_metrics[,which(colnames(pcode_metrics)=="work"):which(colnames(pcode_metrics)=="median_time_finish")],by=list(pcode_metrics$pcode), mean))

#sub_by_pcode_veh_pc<-select(by_pcode_veh_pc, pc_s_ENGINE.RESCUE,pc_s_TRACTOR,pc_s_SWEEPER, pc_s_VACTOR, pc_s_TRUCK)


##Save output as csv
write.csv(by_pcode_veh_pc, "by_pcode_veh_pc.csv")
write.csv(by_pcode_veh_stock, "by_pcode_veh_stock.csv")


pcode_metrics_L<-pcode_metrics %>% 
  select(date, starts_with("pc"))




#################################################################
####-------WORK ORDER METRICS------- COUNT OPEN WORK ORDERS BY DAY, 
###  NEW WORK ORDERS BY DAY, MEAN AND MEDIAN TIME TO FINISH (FOR WORK ORDERS WITH A FINISH DATE, BY DATE OPENED),

### By type of work (repairs vs PM) and by vehicle type
repairs_daily_pcode$month<-month(repairs_daily_pcode$date)
repairs_daily_pcode$year<-year(repairs_daily_pcode$date)
repairs_daily_pcode$year_mon <- format(as.Date(repairs_daily_pcode$date), "%Y-%m")
repairs_daily_pcode$quarter<-quarters(repairs_daily_pcode$date)
repairs_daily_pcode<-data.table(repairs_daily_pcode)

##aggregate by month
monthly<- as.data.frame(repairs_daily_pcode[, j=list(daily_avg_open_all_orders=mean(work, na.rm = TRUE), 
                                                     daily_avg_open_repairs=mean(Repairs, na.rm = TRUE),
                                                     daily_avg_open_pm=mean(PM, na.rm = TRUE)
                                                     # daily_avg_new_orders=mean(new_work_orders, na.rm=TRUE),
                                                     # montly_total_new_orders=sum(new_work_orders, na.rm=TRUE),
                                                     # average_time_finish=mean(average_time_finish, na.rm=TRUE),
                                                     # median_time_finish=mean(median_time_finish, na.rm=TRUE)

), 
by = list(year, month, year_mon, pcode)])

##Turn to long
monthly_rep<-select(monthly, year, month, year_mon, pcode, daily_avg_open_all_orders, daily_avg_open_repairs)
monthly_pm<-select(monthly, year, month,year_mon, pcode, daily_avg_open_all_orders, daily_avg_open_pm)
names(monthly_rep)[6] <- "daily_avg_open_bytype"
monthly_rep$job_type<-"REPAIR"
names(monthly_pm)[6] <- "daily_avg_open_bytype"
monthly_pm$job_type<-"PM"

monthly_op<-rbind(monthly_rep, monthly_pm)
names(monthly_op)[3]<- "year_mon_o"
monthly_op$pcode<-as.character(monthly_op$pcode)
rm(monthly, monthly_rep, monthly_pm)




##For unclosed order counts- Analyze only work orders opened from  "2018-07-01" to "2020-06-30"
work_veh_a<-subset(work_veh, date_opened>="2018-07-01" & date_opened< Sys.Date() )
table(work_veh_a$year_mon_o)
work_veh_a$counter<-1
work_veh_a$not_finished<-ifelse(is.na(work_veh_a$date_finished)==TRUE, 1, 0)
work_veh_a$days_in_month<-as.numeric(lubridate::days_in_month(work_veh_a$date_opened))

# table(work_veh_a$not_finished)
# table(work_veh_a$priority_recode)
# table( work_veh_a$not_finished, work_veh_a$priority_recode)
# 

wo_month_metrics<-work_veh_a %>%
  group_by(priority_recode, year_mon_o, job_type) %>%
  summarize(unclosed=sum(not_finished, na.rm = TRUE),
            monthly_new_orders = sum(counter, na.rm = TRUE),
            daily_avg_new_orders = (sum(counter, na.rm = TRUE)/mean(days_in_month,na.rm=TRUE)),
            avg_days=mean(time_finish, na.rm=TRUE),
            med_days=median(time_finish, na.rm=TRUE),
            avg_labor_cost=mean(labor_cost, na.rm=TRUE),
            total_labor_cost=sum(labor_cost, na.rm=TRUE),
            avg_labor_hours=mean(labor_hours, na.rm=TRUE),
            total_labor_hours=sum(labor_hours, na.rm=TRUE),
            avg_share_labor_hours= mean(share_labor_time, na.rm=TRUE),
            avg_parts_cost=mean(parts_cost, na.rm=TRUE),
            total_parts_cost=sum(parts_cost, na.rm=TRUE),
            avg_total_cost=mean(total_cost, na.rm=TRUE),
            total_total_cost=sum(total_cost, na.rm= TRUE))

##We only care about priority code 1 and 2 for this analysis
wo_month_metrics<-subset(wo_month_metrics, priority_recode==1 | priority_recode==2)
names(wo_month_metrics)[1]<- "pcode"


#join all work order metrics

wo_month_metrics_btype<-left_join(wo_month_metrics, monthly_op, by= c("year_mon_o", "pcode", "job_type"))
write_csv(wo_month_metrics_btype, "wo_month_metrics_btype.csv")

##----------------------------------
############ LOCATION ANALYSIS- OPEN WORK ORDERS (DAILY AVG BY MONTH)
##----------------------------------

##Daily work order metrics by location, by job type
####ANALYSIS BY LOCATION/by job type


##REPAIRS only
work_rep<-subset(work_veh_a, job_type=="REPAIR")
table(work_rep$work_loc)

##Create empty dataframe to fill in with calculated fields


repairs_daily_rep<-NULL #create empty data frame to fill in with calculated fields

for (i in seq_along(dates)) { 
  
  ##Vehicles repaired by day
  p_work_veh<-work_rep
  p_work_veh[, vehicle_work:= ifelse((dates[i]>=as.Date(date_opened) & dates[i]<as.Date(date_finished)) |
                                       (dates[i]>=as.Date(date_opened) & is.na(date_finished)==TRUE),1, 0)] #all orders open
  
  date_work_veh<-subset(p_work_veh, vehicle_work==1) #subset of rows where for date "i" there is work being done on a vehicle
  date_work_veh<-date_work_veh[!duplicated(date_work_veh$equip_id), ] #keep only one row per vehicle, per day
  
  
  
  ##Fill in empty matrix with calculated daily
  repairs_daily_rep<-rbind(repairs_daily_rep, data.frame( "date"= dates[i],
                                                          "Open"= uniqueN(date_work_veh$equip_id, na.rm=TRUE),
                                                          "C-BS"= sum(date_work_veh$work_loc== "C-BS", na.rm=TRUE),
                                                          "C-MC"= sum(date_work_veh$work_loc== "C-MC", na.rm=TRUE),
                                                          "C-SH"= sum(date_work_veh$work_loc== "C-SH", na.rm=TRUE),
                                                          "CH"= sum(date_work_veh$work_loc== "CH", na.rm=TRUE),
                                                          "E-SH"= sum(date_work_veh$work_loc== "E-SH", na.rm=TRUE),
                                                          "FB"= sum(date_work_veh$work_loc== "FB", na.rm=TRUE),
                                                          "MC-SH"= sum(date_work_veh$work_loc== "MC-SH", na.rm=TRUE),
                                                          "MP"= sum(date_work_veh$work_loc== "MP", na.rm=TRUE),
                                                          "N-SH"= sum(date_work_veh$work_loc== "N-SH", na.rm=TRUE),
                                                          "NE-SH"= sum(date_work_veh$work_loc== "NE-SH", na.rm=TRUE),
                                                          "NW-SH"= sum(date_work_veh$work_loc== "NW-SH", na.rm=TRUE),
                                                          "RC"= sum(date_work_veh$work_loc== "RC", na.rm=TRUE),
                                                          "S-SH"= sum(date_work_veh$work_loc== "S-SH", na.rm=TRUE),
                                                          "SDFDRF"= sum(date_work_veh$work_loc== "SDFDRF", na.rm=TRUE),
                                                          "SE-SH"= sum(date_work_veh$work_loc== "SE-SH", na.rm=TRUE),
                                                          "W-SH"= sum(date_work_veh$work_loc== "W-SH", na.rm=TRUE))
                           
                           
                           #count number of unique vehicles vehicles being worked on date "i")
  )
  
  
}

library(reshape2)


# repairs_daily_rep_L <-melt(repairs_daily_rep, id.vars=c("date", "Open"))
# repairs_daily_rep_L$job_type<-"REPAIR"

repairs_daily_rep$job_type<-"REPAIR"




##PM only
work_pm<-subset(work_veh_a, job_type=="PM")
table(work_rep$work_loc)

##Create empty dataframe to fill in with calculated fields


repairs_daily_pm<-NULL #create empty data frame to fill in with calculated fields

for (i in seq_along(dates)) { 
  
  ##Vehicles repaired by day
  p_work_veh<-work_pm
  p_work_veh[, vehicle_work:= ifelse((dates[i]>=as.Date(date_opened) & dates[i]<as.Date(date_finished)) |
                                       (dates[i]>=as.Date(date_opened) & is.na(date_finished)==TRUE),1, 0)] #all orders open
  
  date_work_veh<-subset(p_work_veh, vehicle_work==1) #subset of rows where for date "i" there is work being done on a vehicle
  date_work_veh<-date_work_veh[!duplicated(date_work_veh$equip_id), ] #keep only one row per vehicle, per day
  
  
  
  ##Fill in empty matrix with calculated daily
  repairs_daily_pm<-rbind(repairs_daily_pm, data.frame( "date"= dates[i],
                                                        "Open"= uniqueN(date_work_veh$equip_id, na.rm=TRUE),
                                                        "C-BS"= sum(date_work_veh$work_loc== "C-BS", na.rm=TRUE),
                                                        "C-MC"= sum(date_work_veh$work_loc== "C-MC", na.rm=TRUE),
                                                        "C-SH"= sum(date_work_veh$work_loc== "C-SH", na.rm=TRUE),
                                                        "CH"= sum(date_work_veh$work_loc== "CH", na.rm=TRUE),
                                                        "E-SH"= sum(date_work_veh$work_loc== "E-SH", na.rm=TRUE),
                                                        "FB"= sum(date_work_veh$work_loc== "FB", na.rm=TRUE),
                                                        "MC-SH"= sum(date_work_veh$work_loc== "MC-SH", na.rm=TRUE),
                                                        "MP"= sum(date_work_veh$work_loc== "MP", na.rm=TRUE),
                                                        "N-SH"= sum(date_work_veh$work_loc== "N-SH", na.rm=TRUE),
                                                        "NE-SH"= sum(date_work_veh$work_loc== "NE-SH", na.rm=TRUE),
                                                        "NW-SH"= sum(date_work_veh$work_loc== "NW-SH", na.rm=TRUE),
                                                        "RC"= sum(date_work_veh$work_loc== "RC", na.rm=TRUE),
                                                        "S-SH"= sum(date_work_veh$work_loc== "S-SH", na.rm=TRUE),
                                                        "SDFDRF"= sum(date_work_veh$work_loc== "SDFDRF", na.rm=TRUE),
                                                        "SE-SH"= sum(date_work_veh$work_loc== "SE-SH", na.rm=TRUE),
                                                        "W-SH"= sum(date_work_veh$work_loc== "W-SH", na.rm=TRUE))
                          
                          
                          #count number of unique vehicles vehicles being worked on date "i")
  )
  
  
}



# repairs_daily_pm_L <-melt(repairs_daily_pm, id.vars=c("date", "Open"))
# repairs_daily_pm_L$job_type<-"PM"

repairs_daily_pm$job_type<-"PM"




##Append all location calculations
daily_loc<-rbind(repairs_daily_rep, repairs_daily_pm)
daily_loc$year<-year(daily_loc$date)
daily_loc$month<-month(daily_loc$date)


##aggregate by month
monthly_open_loc<- aggregate(.~year + month + job_type , daily_loc[,-1], mean)
write.csv(monthly_open_loc, "monthly_open_loc.csv")


### CONTROL FOR AVERAGE WORKLOAD BY LOCATION- AVG DAILY NEW ORDERS
work_veh_a<-subset(work_veh_a, priority_recode==1|priority_recode==2)
avg_wo_loc<-work_veh_a %>%
  group_by(year_mon_o, job_type, work_loc) %>%
  summarize(monthly_new_orders = sum(counter, na.rm = TRUE))
avg_wo_loc$year_mon_o<-as.yearmon(avg_wo_loc$year_mon_o)
avg_wo_loc$month<-month(avg_wo_loc$year_mon_o)
avg_wo_loc$year<-year(avg_wo_loc$year_mon_o)


monthly_open_loc_L <-melt(monthly_open_loc, id.vars=c("year", "month", "job_type", "Open"))
monthly_open_loc_L$work_loc<-monthly_open_loc_L$variable
monthly_open_loc_L$all_open<-monthly_open_loc_L$Open
monthly_open_loc_L$Open<-NULL
#str_replace(string, pattern, replacement)

monthly_open_loc_L$work_loc<-str_replace_all(monthly_open_loc_L$variable, "\\.", "-")
monthly_open_loc_L$variable<-NULL
names(monthly_open_loc_L)[4] <- "daily_avg_open_loc"

monthly_open_loc_L<- left_join(monthly_open_loc_L, avg_wo_loc)
monthly_open_loc_L$monthly_new_orders<-ifelse(is.na(monthly_open_loc_L$monthly_new_orders)==TRUE,0, monthly_open_loc_L$monthly_new_orders)

monthly_open_loc_L$daily_avg_open_locN<- monthly_open_loc_L$daily_avg_open_loc/monthly_open_loc_L$monthly_new_orders

monthly_open_loc_L$daily_avg_open_locN<-ifelse(monthly_open_loc_L$monthly_new_orders==0,0, monthly_open_loc_L$daily_avg_open_locN)


summary(monthly_open_loc_L$daily_avg_open_locN)


monthly_open_loc_L$year_mon_o<-NULL

write.csv(monthly_open_loc_L, "monthly_open_loc_L.csv")



### ----BY LOC BY VEHICLE
## Which vehicles are repaired in which locations, relationship between location and underperformance
#  count number of work orders by car by location, calculate percentages

work_veh$counter<-1

work_veh_loc<- work_veh %>%  
  group_by(veh_group, job_type, work_loc) %>%
  summarize(total_orders = sum(counter, na.rm = TRUE),
            avg_days=mean(time_finish, na.rm=TRUE),
            med_days=median(time_finish, na.rm=TRUE),
            avg_labor_cost=mean(labor_cost, na.rm=TRUE),
            total_labor_cost=sum(labor_cost, na.rm=TRUE),
            avg_labor_hours=mean(labor_hours, na.rm=TRUE),
            total_labor_hours=sum(labor_hours, na.rm=TRUE),
            avg_share_labor_hours= mean(share_labor_time, na.rm=TRUE),
            avg_parts_cost=mean(parts_cost, na.rm=TRUE),
            total_parts_cost=sum(parts_cost, na.rm=TRUE),
            avg_total_cost=mean(total_cost, na.rm=TRUE),
            total_total_cost=sum(total_cost, na.rm= TRUE))

##top 3 repair locations by vehicle
# work_veh_loc_r<-subset(work_veh_loc, job_type=="REPAIR")
# library(ggplot2)
# ggplot(work_veh_loc_r, aes(fill=work_loc, y=total_orders, x=veh_group)) + 
#   geom_bar(position="dodge", stat="identity")


#total work orders by veh_group
options(scipen=999)
test<-aggregate(work_veh_loc$total_orders, by=list(veh_group=work_veh_loc$veh_group, job_type=work_veh_loc$job_type), FUN=sum)
work_veh_loc<-left_join(work_veh_loc, test)
work_veh_loc<-work_veh_loc %>% 
  rename(
    total_or_v = x)
work_veh_loc$perc_loc_v<- work_veh_loc$total_orders/work_veh_loc$total_or_v

write.csv(work_veh_loc, "work_veh_loc.csv")


### ########### ####
### End of Code ####
### ########### ###

##run only for discussion:

#####VISUALS####


##monthly  trends
# trace_r1<-monthly_p$daily_avg_open_orders.x
# trace_r2<-monthly_p$daily_avg_open_orders.y
# 
# trace_wn1<-monthly_p$daily_avg_new_orders.x
# trace_wn2<-monthly_p$daily_avg_new_orders.y
# 
# trace_nor1<-monthly_p$montly_total_new_orders.x
# trace_nor2<-monthly_p$montly_total_new_orders.y
# 
# avg_1<-monthly_p$average_time_finish.x
# avg_2<-monthly_p$average_time_finish.y
# 
# med_1<-monthly_p$median_time_finish.x
# med_2<-monthly_p$median_time_finish.y
# 
# pc_unc_1<-monthly_p$pc_unc_1
# pc_unc_2<-monthly_p$pc_unc_2
# 
# 
# x<- as.Date(monthly_p$year_mon)
# 
# ##open work orders
# pw <- plot_ly(monthly_p, x = ~x, y = ~trace_r1, name = 'Open Work Orders, P1', type = 'scatter', mode = 'lines') %>%
#   add_trace(y=trace_r2, name = 'Open Work Orders, P2', type = 'scatter', mode = 'lines') %>%
#   layout(title = "Open Work Orders",
#          xaxis = list(title = "Date"),
#          yaxis = list (title = "Count Open Work Orders", rangemode = "tozero"))
# pw
# 
# ##Average time per order
# tm <- plot_ly(monthly_p, x = ~x, y = ~avg_1, name = 'Average Time per Order P1', type = 'scatter', mode = 'lines') %>%
#   add_trace(y=avg_2, name = 'Average Time per Order P2', type = 'scatter', mode = 'lines') %>%
#   layout(title = "Average Time to Complete Work Orders, by Date Opened",
#          xaxis = list(title = "Date"),
#          yaxis = list (title = "Days Per Finished Order", rangemode = "tozero"))
# tm
# 
# ##Median time per order
# tmed <- plot_ly(monthly_p, x = ~x, y = ~med_1, name = 'Median Time per Order P1', type = 'scatter', mode = 'lines') %>%
#   add_trace(y=med_2, name = 'Median Time per Order P2', type = 'scatter', mode = 'lines') %>%
#   layout(title = "Median Time to Complete Work Orders, by Date Opened",
#          xaxis = list(title = "Date"),
#          yaxis = list (title = "Days Per Finished Order", rangemode = "tozero"))
# tmed
# 
# ####Monthly total new orders
# nor <- plot_ly(monthly_p, x = ~x, y = ~trace_nor1, name = 'New Orders P1', type = 'scatter', mode = 'lines') %>%
#   add_trace(y=trace_nor2, name = 'New Orders P2', type = 'scatter', mode = 'lines') %>%
#   layout(title = "New Orders by Month",
#          xaxis = list(title = "Date"),
#          yaxis = list (title = "Total New Orders", rangemode = "tozero"))
# 
# nor
# 
# 
# ####Unclosed
# unc <- plot_ly(monthly_p, x = ~x, y = ~pc_unc_1, name = 'Percent Unclosed P1', type = 'scatter', mode = 'lines') %>%
#   add_trace(y=pc_unc_2, name = 'Percent Unclosed P2', type = 'scatter', mode = 'lines') %>%
#   layout(title = "P. Unclosed Orders by Month",
#          xaxis = list(title = "Date"),
#          yaxis = list (title = "Proportion of Unclosed Orders", rangemode = "tozero"))
# 
# unc
# 
# ##high percentage of unclosed on July2020
# 
# july20<-subset(work_veh, date_opened>="2020-07-01" & date_opened<="2020-07-31" & is.na(date_closed)==TRUE)
# july20_c<-subset(work_veh, date_opened>="2020-07-01" & date_opened<="2020-07-31" & is.na(date_closed)==FALSE)
# 
# table(july20$work_loc) #predominantly MPT 
# # C-BS   C-SH     CH     FB     FM     MP    MPT     RC SDFDRF   W-SH 
# #    8      1      3      3      4      1    138      1     10      1 
# 
# table(july20_c$work_loc) #None of the closed work orders from July are from MPT
# # C-BS   C-MC   C-SH     CH   E-SH     FB     FI     FM  MC-SH     MP   N-SH  NE-SH  NW-SH     RC   S-SH SDFDRF  SE-SH   W-SH 
# # 18     65    347    925    148     49     36     36     99    945    107     61     31    321     71     69     69     82 
# 
# table(july20$repair_reason) #predominantly repair reason "I" (although I think this variable was not super reliable, it is still telling)
# # A  AW   B   D  DR   E   I   P   S   U   V 
# # 6   1   1   4   5   1 138   1   4   7   2 
# 
# table(july20_c$repair_reason) #none of the closed orders have repair reason I
# # A   AW    B    D   DR    E    F    O    P    Q    R   RE    S    T    U    V    W 
# # 11   18   17   27   55   76   36    5  827  164  198    4 1005    6  962    3    9 
# 
# table(july20$dept_group) #predominantly ESD
# table(july20_c$dept_group) #all over
# 
# table(july20$veh_group) #mostly PACKER-REFUSE and some ENGINE/RESCUE VEHICLES
# # ENGINE/RESCUE         Other PACKER-REFUSE        Patrol            PU       SCOOTER  SEDAN_nonpat    SUV_nonpat         TRUCK 
# #          44             4           106             9             2             1             1             1             2 
# 
# table(july20$std_group) #std group (from std class 87)
# # 11  13  15  16  37  47  57  87  91 
# #  1   5   3   6   1   3   5  145  1 
# 
# 


###old for work order metrics
# work_type<- work_veh_a %>%  
#   group_by(veh_group, job_type) %>%
#   summarize(total_orders = sum(counter, na.rm = TRUE),
#             avg_days=mean(time_finish, na.rm=TRUE),
#             med_days=median(time_finish, na.rm=TRUE),
#             avg_labor_cost=mean(labor_cost, na.rm=TRUE),
#             total_labor_cost=sum(labor_cost, na.rm=TRUE),
#             avg_labor_hours=mean(labor_hours, na.rm=TRUE),
#             total_labor_hours=sum(labor_hours, na.rm=TRUE),
#             avg_share_labor_hours= mean(share_labor_time, na.rm=TRUE),
#             avg_parts_cost=mean(parts_cost, na.rm=TRUE),
#             total_parts_cost=sum(parts_cost, na.rm=TRUE),
#             avg_total_cost=mean(total_cost, na.rm=TRUE),
#             total_total_cost=sum(total_cost, na.rm= TRUE))





