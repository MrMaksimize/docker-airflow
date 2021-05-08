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
from trident.util.salesforce_bulkipy import SalesforceBulkipy
import csv
import json
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from botocore.config import Config

conf = general.config

# is this the best location for this file?
last_timestamp_file = 'reference/case_history_export_last_timestamp'

query_limit = 10000

# if "last id" file exists
#   get case history since "last id"
# else
#   get case history
# write to aws at <case_id>/<history_id>
# write last id to "last id" file

def get_last_timestamp():
    """Get case history from Salesforce, export to AWS."""

    session = boto3.session.Session(profile_name='airflowssm')
    s3 = session.resource('s3')

    #bucket_name = Variable.get('S3_REF_BUCKET')
    bucket_name = Variable.get('S3_GID_CASE_BUCKET')
    obj = s3.Object(bucket_name, last_timestamp_file)

    last_timestamp = obj.get()['Body'].read()

    if last_timestamp != None:
        return last_timestamp.decode()
    else:
        return None


def backup_case_history(**context):

    sf_conn = BaseHook.get_connection(conn_id="DPINT_SF")

    username = sf_conn.login
    password = sf_conn.password
    security_token = sf_conn.extra_dejson

    bulk = SalesforceBulkipy(username=username, password=password, security_token=security_token.get('token'))

    last_timestamp = context['task_instance'].xcom_pull(task_ids='get_last_timestamp')

    if last_timestamp == None:
        last_timestamp = '2021-05-05T15:00:31.000Z' # from manual load

    query = f"SELECT CaseId, CreatedById, CreatedDate, Field, Id, IsDeleted, NewValue, OldValue FROM CaseHistory WHERE CreatedDate > {last_timestamp} ORDER BY CreatedDate LIMIT {query_limit}"

    logging.info(f'Querying case history equal or newer than {last_timestamp}.')
    job = bulk.create_job("CaseHistory", contentType='CSV', operation="queryAll")
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    history = bulk.get_batch_result_iter(job, batch, parse_csv=True, logger=logging.info)

    bucket_name = Variable.get('S3_GID_CASE_BUCKET')
    session = boto3.session.Session(profile_name='airflowssm')
    s3 = session.resource('s3')

    line_num = 0
    new_last_timestamp = None
    for history_line in history:
        obj = s3.Object(bucket_name, history_line['CaseId'] + '/' + history_line['Id'])
        json_history_line = json.dumps(history_line)
        obj.put(Body=json_history_line.encode())

        new_last_timestamp = history_line['CreatedDate']

        line_num = line_num + 1
        #if line_num % 100 == 0:
        #    logging.info(f'Uploaded {line_num} {obj} {new_last_timestamp}')


    if new_last_timestamp != None:
        logging.info(f'new last timestamp: {new_last_timestamp}.')
        obj = s3.Object(bucket_name, last_timestamp_file)
        obj.put(Body=new_last_timestamp.encode())


    return "Successfully pulled Salesforce CaseHistory"

