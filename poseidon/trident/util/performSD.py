import json
import pandas as pd
import boto3
import requests
import os
import pygsheets as pyg
import datetime as dt
import numpy as np

GID_URL2020 = "http://seshat.datasd.org/get_it_done_311/get_it_done_2020_requests_datasd_v1.csv"
GID_URL2019 = "http://seshat.datasd.org/get_it_done_311/get_it_done_2019_requests_datasd_v1.csv"
GID_URL2018 = "http://seshat.datasd.org/get_it_done_311/get_it_done_2018_requests_datasd_v1.csv"
GID_URL2017 = "http://seshat.datasd.org/get_it_done_311/get_it_done_2017_requests_datasd_v1.csv"
GID_URL2016 = "http://seshat.datasd.org/get_it_done_311/get_it_done_2016_requests_datasd_v1.csv"

def sonar():
    def cols_to_read(x):
        return x.lower() not in ['comm_plan_code', 'comm_plan_name','park_name','public_description']
    
    dtype={'service_request_id':str,'service_request_parent_id':str,'sap_notification_number':str,'case_origin':str,'council_district':str}

    df2020 = pd.read_csv(GID_URL2020, usecols=cols_to_read, dtype=dtype, low_memory=False)
    df2019 = pd.read_csv(GID_URL2019, usecols=cols_to_read, dtype=dtype, low_memory=False)
    df2018 = pd.read_csv(GID_URL2018, usecols=cols_to_read, dtype=dtype, low_memory=False)
    df2017 = pd.read_csv(GID_URL2017, usecols=cols_to_read, dtype=dtype, low_memory=False)
    df2016 = pd.read_csv(GID_URL2016, usecols=cols_to_read, dtype=dtype, low_memory=False)
    
    #Concatenate all reporting years
    # df2016todate = pd.concat([df2020,df2019,df2018,df2017,df2016],ignore_index=True, sort=True)
    # df = df2016todate
    
    #Concatenate all reporting years
    df = df2016.append(df2017)
    df = df.append(df2018)
    df = df.append(df2019)
    df = df.append(df2020)
    
    df=df.sort_values(by='date_requested', ascending=True)
    
    #Set variable for GID service request type
    service = "Sidewalk Repair Issue"

    # format df.date_requested
    df.date_requested=pd.to_datetime(df.date_requested).dt.strftime("%m-%d-%Y %H:%M")

    #Delete irrelevant columns from dataframe (prospective function)
    df_trunc = df

    #Drop null values for Council District
    df_trunc_set = df_trunc.loc[(df_trunc['council_district'].isnull())]
    #NaN_Council_dF = df_GID_all[df_GID_all.council_district.isnull()]
    
    df_trunc_set_sidewalk = df_trunc_set.loc[(df_trunc_set['service_name']==service)]
    df_trunc_count = []
    df_trunc_count = len(df_trunc_set_sidewalk)

    #Setup DataFrame for service
    dF_service = df.loc[(df['service_name']==service)]
    
    #De-duplicating parent-child, first identifying children
    dF_service = dF_service.assign(is_child='no')
    dF_service.loc[dF_service['service_request_parent_id'].notnull(), 'is_child'] = 'yes'
    dF_service_parents = dF_service[dF_service.is_child=='no']

    #Narrow to sidealk repair cases with status as "new"
    # New = new
    dF_new_parents = dF_service_parents.loc[(dF_service_parents['status']!='Closed') & (dF_service_parents['status']!='Referred') & (dF_service_parents['status']!='In Process')]
    try:
        asofdate_new_parents = max(dF_new_parents['date_requested'])
    except:
        asofdate_new_parents = 'N/A'
        pass
    new_parents = len(dF_new_parents)
    new_backlog_parents = (f"New: " + str(new_parents) + " cases (" + str(asofdate_new_parents) + ")." "\n")
    
    #Narrow to sidealk repair cases with status as "new" or "in process"
    # New or In Process = NIP
    dF_NIP_parents = dF_service_parents.loc[(dF_service_parents['status']!='Closed') & (dF_service_parents['status']!='Referred')]
    # dF_NIP_parents = dF_NIP_parents.sort_values(by='date_requested', ascending=True)
    asofdate_NIP_parents = min(dF_NIP_parents.date_requested)
    NIP_parents = len(dF_NIP_parents)
    backlog_parents = (f"Backlog: " + str(NIP_parents) + " cases (" + str(asofdate_NIP_parents) + ")."+ "\n")

    spacer = "\n --*---*-- \n"
    snapshot=[]

    #ALERT A: update maximum age of case
    #Iterate over dF rows to re/assign variable for maximum age of case (i.e. exception 
    #reporting to detect negative performance thresholds)
    alertAOC = 0
    snapshot.append(spacer + '/ ' + " OLDEST cases: " + service + " /" + spacer)
    # for rows in dF_NIP_parents.itertuples():
    for index, rows in dF_NIP_parents.iterrows():
        comparison = int(rows['case_age_days'])
        if comparison >= alertAOC:
            alertAOC = int(rows['case_age_days'])
            alertDATE = rows['date_requested']
            alertSRID = rows['service_request_id']
            alertSAP = rows['sap_notification_number']
            snapshot.append(f"Oldest backlog case: " + str(alertAOC) + " days from request date " + str(alertDATE) + "\n")
            snapshot.append(f"SAP#: " + str(alertSAP) + "| GID ID: " + str(alertSRID)+ "\n")
            alertAOC = comparison
        else:
            pass

    #ALERT E: Median age of case (AOC) for cases with status "new" or "in process" exceeds existing maximum median AOC
    dF_NIP_parents.case_age_days.describe()
    dF_NIP_parents.case_age_days.median()
    dF_NIP_parents.case_age_days.mean()
    median_AOC = int(dF_NIP_parents.case_age_days.median())
    mean_AOC=int(dF_NIP_parents.case_age_days.mean())
    snapshot.append(spacer + '/ Backlog STATS: ' + service + ' /' + spacer)
    snapshot.append(f"Median: " + str(median_AOC) + " days."  + "\n" )
    snapshot.append(f"Mean: " + str(mean_AOC) + " days."  + "\n" )

    #Quantiles Age of Case (25th, 75th, 90th, for median is 50th)
    dF_NIP_parents_25th = dF_NIP_parents.quantile(0.25,axis=0)
    dF_NIP_parents_25thAOC = dF_NIP_parents_25th['case_age_days']
    dF_NIP_parents_25thAOC
    
    dF_NIP_parents_75th = dF_NIP_parents.quantile(0.75,axis=0)
    dF_NIP_parents_75thAOC = dF_NIP_parents_75th['case_age_days']
    dF_NIP_parents_75thAOC
    
    dF_NIP_parents_90th = dF_NIP_parents.quantile(0.9,axis=0)
    dF_NIP_parents_90thAOC = dF_NIP_parents_90th['case_age_days']
    dF_NIP_parents_90thAOC

    #ALERT C: service levels
    # For age of case (AOC) > service level agreement (SLA) ("new" or "in proces") build array; 
    # if len(array) > MAX/UL then reassign MAX/UL
    SLA_AOC = 180
    dF_exceed_SLA_SAP = []
    dF_exceed_SLA_dates = []
    dF_exceed_SLA = pd.DataFrame({'id':[], 'sap':[],'requested':[],'age':[],
                              'lat':[],'lng':[],'council':[], 'origin':[]})
    # Count of cases exceeding SLA
    for index, rows in dF_NIP_parents.iterrows():
    # for rows in dF_NIP_parents.itertuples():
        comparison = int(rows['case_age_days'])
        if comparison >= SLA_AOC:
            dF_exceed_SLA_SAP.append(rows['sap_notification_number'])
            dF_exceed_SLA_dates.append(rows['date_requested'])
            dF_exceed_SLA=dF_exceed_SLA.append({'id':rows['service_request_id'],
            'sap':rows['sap_notification_number'],
            'requested':rows['date_requested'],
            'age':rows['case_age_days'],
            'lat':rows['lat'],
            'lng':rows['lng'],
            'council':rows['council_district'],
            'origin':rows['case_origin']}, ignore_index=True)
        else:
            pass
    cnt_exceed_SLA = len(dF_exceed_SLA_SAP)
    oldest_SLA_date = max(dF_exceed_SLA_dates)
    snapshot.append(f"Exceed SLA " + "(" + str(SLA_AOC) + " days): " + str(cnt_exceed_SLA) + " cases."  + "\n" )

    dF_exceed_SLA.rename(columns={'id':'service_request_id',
    'sap':'sap_notification_number',
    'requested':'date_requested',
    'age':'case_age_days',
    'lat':'lat',
    'lng':'lng',
    'council':'council_district',
    'origin':'case_origin'}, inplace=True)
    
    k=0
    snapshot_message = "" 
    # print(type(snapshot_message))
    while k < len(snapshot):
        snapshot_message = str(snapshot_message + snapshot[k])
        k += 1
        
    snapshot_message = str(snapshot_message + backlog_parents + new_backlog_parents)

    # Create dictionary object to set to dataframe (setup to writing to Google Sheet via pygsheets)
    entry_date = dt.datetime.now()
    oldest_case = int(alertAOC)

    hippokampi_dict = {
        'Run Date':[entry_date],
        'Max Age of Case':[oldest_case],
        'Max Age of Case - Date':[alertDATE],
        '25th Percentile': [dF_NIP_parents_25thAOC],
        'Mean Age of Case':[mean_AOC],
        'Median Age of Case':[median_AOC],
        '75th Percentile': [dF_NIP_parents_75thAOC],
        '90th Percentile': [dF_NIP_parents_90thAOC],
        #                    'Max Age of Case - Service Request ID':alertSRID,
        #                    'Max Age of Case - SAP Notification':alertSAP,
        'New Cases':[new_parents],
        #                    'Backlog - Parents':,
        #                    'Children':        
        'Backlog':[NIP_parents],
        'Geolocation error':[df_trunc_count],
        'SLA':[cnt_exceed_SLA],
        'Oldest SLA Date':[oldest_SLA_date]
        #                    'Closed in last 7 days'
    } 
    dF_hippokampi=pd.DataFrame.from_dict(hippokampi_dict)

    # Reading and writing to Google Spreadsheets using Python

    # authorization to access Google sheet
    gc = pyg.authorize(service_file="/usr/local/airflow/poseidon/trident/util/Hippokampi-9c9209b112fe.json")

    #Open the blank Google spreadsheet using Google doc ID key
    pyg_hipp = gc.open_by_key('1CPYX-UymX0qsgBoQ-SctevTYlOiBUJIhooN--uPbxvM')
    
    # Defining columns for backlog dataframe to load to Google sheet
    cols_to_keep = ['service_request_id','sap_notification_number','date_requested',
    'case_age_days','lat','lng','council_district','case_origin']
    dF_backlog = dF_NIP_parents[cols_to_keep].copy()

    # Set Google Sheet containing backlog cases
    geo_wks_backlog =  pyg_hipp[1]
    geo_wks_backlog.set_dataframe(dF_backlog,(1,1))

    # Retrieve Google Sheet containing historicals
    wks = pyg_hipp[0]
    
    #READ - also get the values of sheet as dataframe
    dF_historicals = wks.get_as_df(start=(1,1))

    cols_to_retrieve = ['Run Date', 'Max Age of Case', 'Max Age of Case - Date', 'Mean Age of Case','Median Age of Case','New Cases', 'Backlog', 'Geolocation error','SLA', 'Oldest SLA Date','25th Percentile','75th Percentile','90th Percentile']
    dF_historicals = dF_historicals[cols_to_retrieve]

    # dF_today = dF_hippokampi.append(dF_historicals,ignore_index=True)
    dF_historicals = dF_historicals.append(dF_hippokampi, ignore_index=True, sort=False)

    #CREATE - set the values of a pandas dataframe to sheet
    wks_side =  pyg_hipp[0]
    wks_side.set_dataframe(dF_historicals,(1,1))

    print("Sent message {}".format(snapshot_message))

# SNS publish
"""
    AlertA = sns.publish(
        TopicArn=topic_arn, 
        Message=snapshot_message, 
        Subject="SONAR " + service +  " Service")

    print("Sent message {}".format(snapshot_message))
    print(AlertA)
    return AlertA
"""