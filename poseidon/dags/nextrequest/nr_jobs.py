import requests
import json 
import numpy as np
import pandas as pd
import logging
import pendulum
from trident.util import general
from airflow import AirflowException

#TODO add stuff to general
conf = general.config
next_request_token = conf['nextrequest_token']

nextrequest_closed_temp = conf['temp_data_dir'] + '/nextrequest_closed.csv'
nextrequest_closed_prod = conf['prod_data_dir'] + '/nextrequest_closed.csv'

nextrequest_open_temp = conf['temp_data_dir'] + '/nextrequest_open.csv'
nextrequest_open_prod = conf['prod_data_dir'] + '/nextrequest_open.csv'

def request_pra_date(**context):
    #TODO add context execution date logic

    request_url = 'https://sandiego.nextrequest.com/api/v2/requests?start_date={}'.format(start_date)
    r = requests.get(request_url, headers={'Authorization':next_request_token})
    json_r = r.text
    parsed = json.loads(json_r)
    df_json = pd.read_json(json.dumps(parsed, indent=4, sort_keys=True))
    df_final = df_json[['id', 'pretty_id', 'request_date', 'closed_date', 'closure_reasons', 'state', 'initial_contact_date', 'general_report_response_days', 'days_to_close', 'biz_days_to_close']]
    df_final = df_final.rename(columns={'pretty_id':'request_number', 'state':'status', 'general_report_response_days':'days_to_response'})
    df_final = df_final.set_index('id')
    #return df_final

    df_final_closed = df_final[df_final['status']=='Closed']
    df_final_open = df_final[df_final['status']!='Closed']

	logging.info(f"Writing {interval} data to temp closed file")
	general.pos_write_csv(df_final_closed, nextrequest_closed_temp, index=True, date_format=conf['date_format_ymd_hms'])

	logging.info(f"Writing {interval} data to temp open file")
	general.pos_write_csv(df_final_open, nextrequest_open_temp, index=True, date_format=conf['date_format_ymd_hms'])

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
	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_prod.shape[0]
	logging.info(f'Writing to production {results} rows in {prod_file}')
	return f"Successfully wrote prod file with {results} records"