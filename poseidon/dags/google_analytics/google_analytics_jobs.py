""" Google Analytics _jobs file """

# Required imports

from trident.util import general
import logging

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

# -- Imports for transformations

import pandas as pd
import json
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pendulum
import re

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']
secrets_str = conf['ga_client_secrets']
cols_re = re.compile(r"([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))")

#: Helper function
def getMetrics(report):
    """ This function pulls the rows for the metrics """
    columnHeader = report.get('columnHeader', {})
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    metricHeadersList = []
    for metric in metricHeaders:
        metricHeadersList.append(metric['name'])
    met_no = len(metricHeadersList)
    data = report.get('data', {}).get('rows', [])
    df_rows = []
    for d in data:
        metrics = d['metrics'][0]['values']
        dict_row = {}
        for i in range(met_no):
            dict_row[metricHeadersList[i]] = metrics[i]
        df_rows.append(dict_row)

    cols = [re.sub(cols_re,r"\1_",x[3:]).lower() for x in metricHeadersList]
        
    df = pd.DataFrame(df_rows,columns=metricHeadersList)

    df.columns = cols

    return df

#: Helper function
def getDimensions(report):
    """ This function pulls the rows for dimensions """
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    dim_no = len(dimensionHeaders)
    data = report.get('data', {}).get('rows', [])
    logging.info(report.get('data', {}).get('rowCount', []))
    df_rows = []
    for d in data:
        dimensions = d['dimensions']
        dict_row = {}
        for i in range(dim_no):
            dict_row[dimensionHeaders[i]] = dimensions[i]
        df_rows.append(dict_row)

    cols = [re.sub(cols_re,r"\1_",x[3:]).lower() for x in dimensionHeaders]
        
    df = pd.DataFrame(df_rows,columns=dimensionHeaders)

    df.columns = cols

    return df


#: Dag function
def create_client_secrets():
    """ 
    Authenticate Google Analytics client
    """
    logging.info("Writing keyfile to authenticate GA")

    json_path = f'{temp_path}/client_secrets_v4.json'

    secrets_json = json.loads(secrets_str)
    
    with open(json_path, 'w') as json_file:
        json.dump(secrets_json, json_file)

    return "Successfully wrote keyfile for authentication"
        
#: Dag function
def ga_batch_get(view_id="",
    mets=[],
    dims=[],
    out_path="",
    range="",
    **context):
    """ 
    Run a batch get for a specific analytics report
    View ID is the ID for the analytics property
    Mets is the list of metrics objects
    Dims is the list of dimension objects

    """
    
    logging.info("authenticating using keyfile")

    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = f'{temp_path}/client_secrets_v4.json'
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    exec_date = context['next_execution_date']
    end = exec_date.strftime('%Y-%m-%d')

    # Working backward, subtract range

    if range == "monthly":

        start = exec_date.subtract(months=1).strftime('%Y-%m-%d')

    elif range == "weekly":

        start = exec_date.subtract(weeks=1).strftime('%Y-%m-%d')

    elif range == "daily":

        start = exec_date.subtract(days=1).strftime('%Y-%m-%d')

    else:

        raise Exception("Range is not specified")
    
    #start = exec_date.strftime('%Y-%m-%d')
    #end = exec_date.add(months=1).strftime('%Y-%m-%d')
    
    

    logging.info("Creating metrics dictionary")

    metrics = []
    dimensions = []

    for m in mets:
        metrics.append({'expression':f'ga:{m}'})

    for d in dims:
        dimensions.append({'name':f'ga:{d}'})

    logging.info("Running batch request")
    logging.info(f"Starting at {start} and ending at {end}")

    response = analytics.reports().batchGet(
    body={
        'reportRequests': [
                {'viewId': view_id,
                'dateRanges': [{'startDate': start,'endDate': end}],
                'samplingLevel': 'LARGE',
                'metrics': metrics,
                'dimensions': dimensions,
                 'pageSize':100000
                }
            ]}).execute()

    logging.info("Processing batch response")

    report = response.get('reports')

    dims_df = getDimensions(report[0])
    mets_df = getMetrics(report[0])
    df = pd.merge(dims_df,mets_df,how="left",left_index=True,right_index=True)

    logging.info("Reading in prod file")

    prod_df = pd.read_csv(f"{prod_path}/{out_path}_datasd.csv",
        low_memory=False,
        dtype={'hour':str}
        )

    # Dedupe will not work without making formats consistent
    df['date'] = pd.to_datetime(df['date'],errors='coerce',format="%Y-%m-%d")
    prod_df['date'] = pd.to_datetime(prod_df['date'],errors='coerce',format="%Y-%m-%d")

    if "hour" in dims:
        # Again, need consistency
        df['hour'] = df['hour'].apply(lambda x: str(x))

    logging.info(f"Prod file has {prod_df.shape[0]} records")

    logging.info("Appending new records, deduping, and sorting")

    # Combine new records with existing prod file
    concat_df = pd.concat([df,prod_df],ignore_index=True,sort=False)
    logging.info(f"Final file has {concat_df.shape[0]} records before dedupe")

    # Deduplicate on dimension columns, but first alter dims list to match
    dedupe_cols = [re.sub(cols_re,r"\1_",x).lower() for x in dims]
    final_df = concat_df.drop_duplicates(dedupe_cols)
    logging.info(f"Final file has {final_df.shape[0]} records after dedupe")
    
    # Sort by time
    if "hour" in dims:
        final_df = final_df.sort_values(['date','hour'])
    else:
        final_df = final_df.sort_values(['date'])
    
    general.pos_write_csv(
        final_df,
        f"{prod_path}/{out_path}_datasd.csv",
        date_format=conf['date_format_ymd'])

    return f"Successfully process GA report for {out_path}"