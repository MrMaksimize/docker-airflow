"""_jobs file for Get It Done."""
import os
import boto3
import pandas as pd
import geopandas as gpd
import logging
import datetime as dt
import numpy as np
from trident.util import general
from trident.util.sf_client import Salesforce
import csv
import json
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

conf = general.config

temp_streets_gid = conf['temp_data_dir'] + '/gid_temp_streetdiv.csv'

# if "last id" file exists
#   get case history since "last id"
# else
#   get case history
# write to aws at <case_id>/<history_id>
# write last id to "last id" file

def get_case_history():
    """Get requests from sf, creates prod file."""
    sf_conn = BaseHook.get_connection(conn_id="DPINT_SF")

    username = sf_conn.login
    password = sf_conn.password
    security_token = sf_conn.extra_dejson

    bulk = SalesforceBulkipy(username=username, password=password, security_token=security_token)

#    bucket_name=Variable.get('S3_REF_BUCKET')
#    s3_url = f"s3://{bucket_name}/reference/{sap_gid}"
#    gid_sap_dates = pd.read_csv(s3_url,dtype={'case_number':str,
#        'sap_notification_number':str})
    last_id = '017t000002k2C4QAAU'

    query = f"Select CaseId, CreatedById, CreatedDate, Field, Id, IsDeleted, NewValue, OldValue FROM CaseHistory WHERE Id > '{last_id}' order by Id limit 100"


    logging.info(f'Process report {last_id} data.')
    job = bulk.create_query_job("CaseHistory", contentType='CSV')
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    # Result
    results = bulk.get_batch_result_iter(job, batch, parse_csv=True)

    print('GOT RESULT')
    print(results)

    for result in results:
        print('ROW')
        print(result) # dictionary rows


    # Pull dataframe

    sf.get_report_csv(report_id, temp_streets_gid)

    logging.info(f'Process report {report_id} data.')

    return "Successfully pulled Salesforce report"

