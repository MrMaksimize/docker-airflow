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

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']
secrets_str = conf['ga_client_secrets']

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
        
    df = pd.DataFrame(df_rows,columns=metricHeadersList)

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
        
    df = pd.DataFrame(df_rows,columns=dimensionHeaders)

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
    **context):
    """ 
    Run a batch get for a specific analytics report
    View ID is the ID for the analytics property
    Start and End are strings of dates
    Mets is the list of metrics objects
    Dims is the list of dimension objects
    page is the page size

    """
    
    logging.info("authenticating using keyfile")

    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = 'client_secrets_v4.json'
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    start = context['execution_date'].strftime('%Y-%m-%d')
    end = start.add(months=1).strftime('%Y-%m-%d')

    logging.info("Creating metrics dictionary")

    metrics = []
    dimensions = []

    for m in mets:
        metrics.append({'expression':f'ga:{m}'})

    for d in dims:
        dimensions.append({'name':f'ga:{d}'})

    logging.info("Running batch request")

    response = analytics.reports().batchGet(
    body={
        'reportRequests': [
                {'viewId': view_id,
                'dateRanges': [{'startDate': start,'endDate': end}],
                'samplingLevel': 'LARGE',
                'metrics': metrics,
                'dimensions': dimensions,
                 'pageSize':page
                }
            ]}).execute()

    logging.info("Processing batch response")

    dims = getDimensions(report)
    mets = getMetrics(report)
    df = pd.merge(dims,mets,how="left",left_index=True,right_index=True)
    
    general.pos_write_csv(
        df,
        f"{prod_path}/{out_path}.csv",
        date_format=conf['date_format_ymd'])

    return "Successfully completed context function"