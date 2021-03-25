import requests
import json 
import numpy as np
import pandas as pd
import logging
import pendulum
from datetime import datetime
from io import StringIO
from trident.util import general
from airflow import AirflowException
from airflow.models import Variable

conf = general.config
next_request_token = Variable.get("NR_TOKEN")

nr_prod_csv = conf['prod_data_dir'] + '/nextrequest_prod.csv'
nr_temp_csv = conf['temp_data_dir'] + '/nextrequest_temp.csv'
nr_sf_csv = conf['prod_data_dir'] + '/nextrequest_sf.csv'

#: DAG Function
def request_pra_date(**context):
    """
    Use execution date to gather 2 months of requests
    """
    pend_request_date = context['execution_date'].in_timezone('America/Los_Angeles')
    pend_request_date_delta = pend_request_date.subtract(months=6)

    request_date = pendulum_to_datetime(pend_request_date)
    request_date_delta = pendulum_to_datetime(pend_request_date_delta)

    df_final = pd.DataFrame()

    while request_date_delta[0:10] != request_date[0:10]:
        logging.info("Starting date: {}".format(request_date_delta))
        df_temp = request_pra_by_date(request_date_delta)
        df_final = combine_and_dedupe(df_final, df_temp)
        previous_date = request_date_delta
        request_date_delta = df_temp.iloc[[-1]]['request_date'].values[0][0:10]

        if request_date_delta == previous_date:
            break
        else:
            previous_date = request_date_delta

        logging.info("NEW starting date: {}".format(request_date_delta))

    logging.info(f"Writing {df_final.shape[0]} rows of data to temp file {nr_temp_csv}")
    #logging.info(df_final)
    general.pos_write_csv(df_final, nr_temp_csv, index=False, date_format="%Y-%m-%d %H:%M:%S")
        
    return f"Successfully wrote temp file"

#: Helper Function for API cal
def request_pra_by_date(start_date):
    request_url = 'https://sandiego.nextrequest.com/api/v2/requests?start_date={}'.format(start_date)
    logging.info(f"Making request for {start_date}")
    r = requests.get(request_url, headers={'Authorization':next_request_token})
    json_r = r.text
    parsed = json.loads(json_r)
    df_json = pd.read_json(StringIO(json.dumps(parsed, indent=4, sort_keys=True)))
    df_final = df_json[['id', 'pretty_id', 'request_date', 'closed_date', 'closure_reasons', 'state', 'initial_contact_date', 'general_report_response_days', 'days_to_close', 'biz_days_to_close']]
    df_final = df_final.rename(columns={'pretty_id':'request_number', 'state':'status', 'general_report_response_days':'days_to_response'})
    logging.info(f"Last date of current batch is {df_final.iloc[[-1]]['request_date'].values[0][0:10]}")
    return df_final

#: Helper Function
def combine_and_dedupe(df1, df2):
    df_final = pd.concat([df1,df2])
    df_final = df_final.drop_duplicates(subset = ['id'],keep='last')
    
    return df_final

#: Helper Function for datetimes
def pendulum_to_datetime(pendulum_date):
    """
    Convert pendulum to datetime format.

    The conversion is done from pendulum -> string -> dateime.

    Args:
        pendulum_date (pendulum): The date you wish to convert.

    Returns:
        (datetime) The converted date.
    """
    fmt = '%Y-%m-%dT%H:%M:%S%z'
    string_date = pendulum_date.strftime(fmt)
    #return datetime.strptime(string_date, fmt)
    return string_date

#: DAG Function
def update_prod(**context):
    df1 = pd.read_csv(nr_temp_csv)
    df2 = pd.read_csv(nr_prod_csv)

    df_final = pd.concat([df2,df1])
    df_final = df_final.drop_duplicates(subset = ['id'],keep='last')

    logging.info(f"Writing {df_final.shape[0]} rows of data to prod file")
    general.pos_write_csv(df_final, nr_prod_csv, index=False, date_format="%Y-%m-%d %H:%M:%S")
    
    return "Successfully wrote prod file"