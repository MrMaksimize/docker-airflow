#Clear workspace
rm(list = ls())


library(data.table)
library(plyr)
library(dplyr)
library(plotly)
#library(readr)
library(tidyr)
library(zoo)
library(stringr)

#====================Load data=============================== 

vehicles<-read.csv("/data/prod/fleet_vehicles.csv")
dept_look<-read.csv("/data/prod/fleet-dept-lookup.csv")

#=========== ID's for vehicles and for work orders

###Vehicles dataset
vehicles<- vehicles %>% mutate_all(na_if,"") #turn blanks into NA
length(unique(vehicles$equip_id))
#[1] 14262 #no duplicate id's

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
write.csv(valid_veh, "/data/prod/fleet_valid_vehicles.csv", row.names=FALSE)