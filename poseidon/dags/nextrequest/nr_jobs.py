import requests
import json 
import numpy as np
import pandas as pd
import logging
import pendulum
from trident.util import general
from airflow import AirflowException

conf = general.config
next_request_token = conf['nextrequest_token']

nextrequest_closed_temp = conf['temp_data_dir'] + '/nextrequest_closed.csv'
nextrequest_closed_prod = conf['prod_data_dir'] + '/nextrequest_closed.csv'

nextrequest_open_temp = conf['temp_data_dir'] + '/nextrequest_open.csv'
nextrequest_open_prod = conf['prod_data_dir'] + '/nextrequest_open.csv'

def get_nr_data_write_temp(**context):
    #TODO FIX PENDULUM DATE FORMAT
	currTime = context['execution_date'].in_timezone('America/Los_Angeles')
	#currTime = currTime.replace(tzinfo=None)
	
    api_start_date = currTime.subtract(days=30)
    api_end_date = currTime.subtract(days=1)

    df_temp_from_api = request_pra_data(api_start_date)
    df_temp_from_api = df_temp_from_api.set_index('id')

    #TODO currTime as string format for API
    while api_start_date[0:7] != api_end_date:
        try:
            df_temp = request_pra_data(api_starting_date)
            df_temp = df_temp.set_index('id')
            df_temp_from_api = combine_and_dedupe(df_temp_from_api, df_temp)
            start_date = df_temp.iloc[[-1]]['request_date'].values[0][0:10]
        except Exception as e:
            continue

    df_temp_from_api_closed = df_temp_from_api[df_temp_from_api['status']=='Closed']
    df_temp_from_api_open = df_temp_from_api[df_temp_from_api['status']!='Closed']

	logging.info(f"Writing {df_temp_from_api_closed.shape[0]} data to temp closed file")
	general.pos_write_csv(df_temp_from_api_closed, nextrequest_closed_temp, index=True, date_format=conf['date_format_ymd_hms'])

	logging.info(f"Writing {df_temp_from_api_open.shape[0]} data to temp open file")
	general.pos_write_csv(df_temp_from_api_open, nextrequest_open_temp, index=True, date_format=conf['date_format_ymd_hms'])	
	
	return f"Successfully wrote temp files"

#Helper Function
def request_pra_data(start_date):
    '''
    Helper function that calls API only
    '''
    #TODO add context execution date logic

    request_url = 'https://sandiego.nextrequest.com/api/v2/requests?start_date={}'.format(start_date)
    r = requests.get(request_url, headers={'Authorization':next_request_token})
    json_r = r.text
    parsed = json.loads(json_r)
    df_json = pd.read_json(json.dumps(parsed, indent=4, sort_keys=True))
    df_temp_from_api = df_json[['id', 'pretty_id', 'request_date', 'closed_date', 'closure_reasons', 'state', 'initial_contact_date', 'general_report_response_days', 'days_to_close', 'biz_days_to_close']]
    df_temp_from_api = df_temp_from_api.rename(columns={'pretty_id':'request_number', 'state':'status', 'general_report_response_days':'days_to_response'})
    df_temp_from_api = df_temp_from_api.set_index('id')
    
    return df_temp_from_api

#Helper Function
def combine_and_dedupe(df1, df2):
    try:
        df1 = df1.set_index('id')
        df2 = df2.set_index('id')
    except KeyError:
        pass
    df_final = pd.concat([df1,df2])
    df_final = df_final.drop_duplicates(keep='first')
    
    return df_final

def update_prod(**context):
    try:
        #Closed Cases
        build_production_files(nextrequest_closed_prod, nextrequest_closed_temp)

        #Open Cases
        build_production_files(nextrequest_open_prod, nextrequest_open_temp)
        
        logging.info("Successfully wrote production files")
    except Exception as e:
        logging.info(f"Something went wrong writing to prod file")

#: Helper Function
def build_production_files(prod_file, temp_file, **context):

	logging.info("Reading prod and temp files")
	df_prod = pd.read_csv(prod_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(temp_file,low_memory=False,index_col=0)

	df_prod = pd.concat([df_prod,df_temp])

	# Index is timestamp
	# Group by timestamp to deduplicate by keeping first
	df_prod = df_prod.drop_duplicates(keep='first')

    if prod_file == nextrequest_open_prod:
        df_closed_id = pd.read_csv(nextrequest_closed_prod,low_memory=False,index_col=0)
        closed_id = df_closed_id['id'].values
        df_prod = df_prod[~df_prod['id'].isin(closed_id)]

	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_prod.shape[0]
	logging.info(f'Writing to production {results} rows in {prod_file}')
	return f"Successfully wrote prod file with {results} records"

#TODO Upload to S3 and Snowflake