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

temp_streets_gid = conf['temp_data_dir'] + '/gid_temp_streetdiv.csv'

# is this the best location for this file?
last_id_file = '/reference/gid/case_history_export_last_id.txt'

# if "last id" file exists
#   get case history since "last id"
# else
#   get case history
# write to aws at <case_id>/<history_id>
# write last id to "last id" file

def get_case_history1():
    """Get case history from Salesforce, export to AWS."""

    bucket_name = Variable.get('S3_REF_BUCKET')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, last_id_file)
    try:
        last_id = obj.get()['Body'].read()
    except:
        last_id = ''



    sf_conn = BaseHook.get_connection(conn_id="DPINT_SF")

    username = sf_conn.login
    password = sf_conn.password
    security_token = sf_conn.extra_dejson

    bulk = SalesforceBulkipy(username=username, password=password, security_token=security_token.get('token'))

    query_limit = 100 #Variable.get('GID_CASE_HISTORY_QUERY_LIMIT')
    if last_id == '':
        query = f"SELECT CaseId, CreatedById, CreatedDate, Field, Id, IsDeleted, NewValue, OldValue FROM CaseHistory ORDER BY Id LIMIT {query_limit}"
    else:
        query = f"SELECT CaseId, CreatedById, CreatedDate, Field, Id, IsDeleted, NewValue, OldValue FROM CaseHistory WHERE Id > '{last_id}' ORDER BY Id LIMIT {query_limit}"

def get_case_history():
    dev = boto3.session.Session(profile_name='airflowssm')
    s3 = dev.resource('s3')

    bucket_name = Variable.get('S3_DATA_BUCKET')
    print('BuCKET')
    print(bucket_name)

    example = {'CaseId': '500t000000KyLHhAAN', 'CreatedById': '005t0000000F4uMAAS', 'CreatedDate': '2019-07-26T00:01:25.000Z', 'Field': 'LayerId__c', 'Id': '017t00000A0M02lAQC', 'IsDeleted': 'false', 'NewValue': '40', 'OldValue': ''}

    obj = s3.Object(bucket_name, example['CaseId'] + '/' + example['Id'])
    try:
        print('FOUND IT')
        print(obj.get()['Body'].read())
    except:
        print('DiDNOT FIND IT')


def noting():
    logging.info(f'Process report data last id: {last_id}.')
    job = bulk.create_query_job("CaseHistory", contentType='CSV')
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    history = bulk.get_batch_result_iter(job, batch, parse_csv=True)

    for history_line in history:
        obj = s3.Object(bucket_name, history_line['CaseId'] + '/' + history_line['Id'])
        json_history_line = json.dumps(history_line)
        obj.put(Body=json_history_line.encode())

    return "Successfully pulled Salesforce CaseHistory"


def nothing():
    logging.info(f'Process report data last id: {last_id}.')
    job = bulk.create_query_job("CaseHistory", contentType='CSV')
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    # Result
    results = bulk.get_batch_result_iter(job, batch, parse_csv=True)

    print(results)


    bucket_name = Variable.get('S3_DATA_BUCKET')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, last_id_file)

    bucket_name = Variable.get('S3_REF_BUCKET')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, last_id_file)
    obj.put

    dest_bucket = self.dest_s3_bucket
    local_fpath = "%s/%s" % (self.source_base_path, self.source_key)

    logging.info("Using conn id to account")

    dest_s3 = S3Hook(aws_conn_id='S3DATA')

    if conf['env'] == 'PROD':
        url = f"http://datasd.prod.s3.amazonaws.com/{self.dest_s3_key}"
    else:
        url = f"http://datasd.dev.s3.amazonaws.com/{self.dest_s3_key}"

    logging.info("%s >>>>> %s/%s" %
                     (local_fpath, dest_bucket, self.dest_s3_key))

    dest_s3.load_file(
        filename=local_fpath,
        key=self.dest_s3_key,
        bucket_name=dest_bucket,
        replace=self.replace)
    logging.info("Upload completed")

    logging.info("URL: {}".format(url))
    s3_file = boto3.client('s3')
    self.verify_file_size_match(s3_file, local_fpath, url)
    
    return url



