import requests
import json 
import numpy as np
import pandas as pd
import logging
import pendulum
from io import StringIO
from trident.util import general
from airflow import AirflowException
from airflow.models import Variable

conf = general.config
next_request_token = Variable.get("NR_TOKEN")

nr_prod_csv = conf['temp_data_dir'] + '/nextrequest_prod.csv'
nr_temp_csv = conf['prod_data_dir'] + '/nextrequest_temp.csv'

"""
Notes:
Run Daily
"""

#: DAG Function
def request_pra_date(**context):
    """
    Use execution date to gather last 100 PRA requests
    """
    request_date = context['execution_date'].in_timezone('America/Los_Angeles')

    request_url = 'https://sandiego.nextrequest.com/api/v2/requests?start_date={}'.format(start_date)
    r = requests.get(request_url, headers={'Authorization':next_request_token})
    json_r = r.text
    parsed = json.loads(json_r)
    df_json = pd.read_json(StringIO(json.dumps(parsed, indent=4, sort_keys=True)))
    df_final = df_json[['id', 'pretty_id', 'request_date', 'closed_date', 'closure_reasons', 'state', 'initial_contact_date', 'general_report_response_days', 'days_to_close', 'biz_days_to_close']]
    df_final = df_final.rename(columns={'pretty_id':'request_number', 'state':'status', 'general_report_response_days':'days_to_response'})#df_final = df_final.set_index('id')
    logging.info(f"Writing {df_final.shape[0]} data to temp file")
    general.pos_write_csv(df_final, nr_temp_csv, index=False, date_format=conf['date_format_ymd_hms'])
        
    return f"Successfully wrote temp file"

#: DAG Function
def update_prod():
    df1 = pd.read_csv(nr_temp_csv)
    df2 = pd.read_csv(nr_prod_csv)

    df_final = pd.concat([df1,df2])
    df_final = df_final.drop_duplicates(subset = ['id'],keep='last')

    logging.info(f"Writing {df_final.shape[0]} data to prod file")
    general.pos_write_csv(df_final, nr_prod_csv, index=False, date_format=conf['date_format_ymd_hms'])
    
    return "Successfully wrote prod file"


#TODO Upload to S3 and Snowflake