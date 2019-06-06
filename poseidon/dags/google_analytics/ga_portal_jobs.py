""" Template _jobs file """

# Required imports

from trident.util import general
import logging

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
import cx_Oracle
from trident.util.sf_client import Salesforce
import ftplib
import subprocess
from subprocess import Popen, PIPE
import requests
import glob
import os
import boto3

# -- Imports for transformations

import pandas as pd
import numpy as np
import math

from collections import OrderedDict
import json
from lxml import etree
from bs4 import BeautifulSoup as bs

import string
import re #regex

import datetime as dt
import time
from dateutil.parser import parse

from shapely.geometry import Point
from trident.util import geospatial
from trident.util.geospatial import geocode_address_google
from trident.util.geospatial import get_address_for_apn
from trident.util.geospatial import spatial_join_pt
import geopandas as gpd
import pymssql

# Optional variables

cur_year = dt.datetime.now().strftime('%Y') # Current year as a string
cur_year = dt.datetime.now().year # Current year as a number
cur_mon = dt.datetime.now().month # Current month as a number
fy = general.get_FY_year() # Current fiscal year as a number

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# -- Oracle connx needs a connection variable in general

credentials = general.source['cip']
credentials = general.source['dsd_permits']
credentials = general.source['ttcs']
credentials = general.source['cef']

# Connection functions

# -- Connect to a shared drive for one file

def shared_drive_file():
    """Get a single known file from a shared drive."""
    fy = 'FY18-19'
    logging.info('Retrieving data for current FY.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        # Change directory to your specific directory
        + "'cd \"TSW-TEO-Shared/TEO/" \
        + "TEO-Transportation-Systems-and-Safety-Programs/" \
        + "Traffic Data/{fy}/RECORD FINDER\";" \
        # Specify file to get and rename if needed
        + " ls; get Machine_Count_Index.xlsx {temp_dir}/{out_f}.xlsx;'"

    command = command.format(adname=conf['mrm_sannet_user'],
                             adpass=conf['mrm_sannet_pass'],
                             fy=fy,
                             temp_dir=temp_path,
                             out_f='traffic_counts_file')

    logging.info(command)

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(output)
    else:
        return 'Successfully retrieved {} data.'.format(fy)

# -- Connect to a shared drive for multiple files

def shared_drive_mget():
    """Use mget on a shared drive."""
    logging.info('Retrieving files on a shared drive using mget.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        # Change directory to your specific directory
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/\";" \
        # Specify where to dump the files
        + " lcd \"/data/temp/\";" \
        # Use a wildcard and a file extension
        + " mget *.xlsx'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=temp_path)

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

# - Connect to a MsSql database

def get_mssql_data():
    """Get data from a mssql database"""
    logging.info('Retrieving data from MsSql database')
    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    query = general.file_to_string('./sql/query.sql', __file__)
    # Include a named connection ID from the set of environment variables
    conn = MsSqlHook(mssql_conn_id='conn_id')
    df = conn.get_pandas_df(query)
    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df, 
        temp_path+'/output_file.csv')

    return "Retrieved mssql data to temp file"

# - Connect to an Oracle database

def get_oracle_data():
    """Query an oracle database"""
    logging.info('Retrieving data from Oracle database')
    # This requires that otherwise optional credentials variable
    db = cx_Oracle.connect(credentials)
    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    sql = general.file_to_string('./sql/query.sql', __file__)
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query returned {df.shape[0]} results')
    
    general.pos_write_csv(
        df,
        temp_path+'/output_file.csv')

    return 'Successfully retrieved Oracle data.'

# - Connect to our ftp site

def get_datasd_ftp():
    """Download files from our FTP."""
    # This is the path to the temp file you'll write to
    filename = conf['temp_data_dir']+'/temp_collisions.csv'
    file = open(filename, 'wb')
    ftp = ftplib.FTP('ftp.datasd.org')

    ftp.login(user=conf['ftp_datasd_user'], passwd=conf['ftp_datasd_pass'])
    # This is the path you want in the ftp
    ftp.cwd('uploads/sdpd/collisions')
    ls = []
    # This returns a directory listing
    # t switch is a nonstandard way to sort by time
    ftp.dir('-t', ls.append)
    # Assign the name of the latest file to name var
    name = ls[0].split()[8]
    # Write the bytes of the latest file to temp file
    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved ftp data."

# - Make a post request to an API

def api_post_req():
	""" Send a post request with params """
	# Build the URL
	req_url = "https://netfile.com:443/" \
                + "Connect2/api/public/" \
                + "campaign/export/cal201/" \
                + "transaction/year"
    # Use requests library to make post request
    # pass in any needed params or data
    apiRequest = requests.post(req_url,
                                #params={'format':'json'},
                                #data = {'Aid':'CSD',
                                        #'Year':cur_yr,
                                        #'CurrentPageIndex':0,
                                        #'PageSize':'1',
                                        #'TransactionType':forms['460A'],
                                        #'ShowSuperceded':'false'}
                                )

    if apiRequest.status_code != 200:
    	# Task failure on unsuccessful request
    	raise Exception("Post request unsuccessful")
    else:
    	logging.info("Post request successful")
    	request_json = apiRequest.json()['results']
    	# Continue by parsing json

   	return "Successfully made an api post request"

# - Make a get request to an API

def api_get_req():
	""" Send a simple get request """
	url = ""
	apiRequest = requests.get(url) 

	if apiRequest.status_code != 200:
    	# Task failure on unsuccessful request
    	raise Exception("Get request unsuccessful")
    else:
    	logging.info("Get request successful")
    	request_json = apiRequest.json()['results']
    	# Continue by parsing json

   	return "Successfully made an api get request"

# Transformation snippets

# - List out files in temp folder

temp_files = [f for f in os.listdir(temp_path)]

# - List out files in prod folder

prod_files = [f for f in os.listdir(prod_path)]

# - Grab a set of files using a wildcard

file_name = f"{temp_path}/file_{other}_{value}_{as_needed}*.csv"
files = glob.glob(file_name)
data = []
for file in files:
	file_df = pd.read_csv(file)
	data.append(df)
	logging.info(f'Read {file}')
df = pd.concat(data,ignore_index=True)

# - Writing a dataframe to prod folder

general.pos_write_csv(
	df,
	f"{prod_path}/{prod_file_name}", # name of the csv output
	date_format=conf['date_format_ymd_hms'])

# - Lowercase columns and replace things like spaces and slashes

df.columns = [x.lower().replace(' ','_').replace('/','_') 
        for x in df.columns]

# - Pull in 1 or more reference files

files = []
s3 = boto3.resource('s3')
s3_ref = s3.Bucket('datasd-reference')
for obj in s3_ref.objects.all():
    if obj.key.startswith('folder/name_prefix_'):
        files.append(obj.key)

# Spatially join points to polygons

spatial_join_result = spatial_join_pt(point_file,
                             poly_file,
                             lat='lat',
                             lon='lng')

# Separate a dataframe into calendar years for prod files
# Date columns must be datetime columns

min_year = df['date_col'].min().year 
max_year = df['date_col'].max().year

for year in range(min_year,max_year+1):
	file_name = f"{prod_path}/dataset_name_{year}_datasd.csv"
	this_yr = dt.datetime(year,1,1,0,0,0)
	next_yr = dt.datetime(year+1,1,1,0,0,0)
	file_subset = df[(df['date_col'] >= this_yr)&(df['date_col'] < next_yr)]
	# Write the subset to csv using file path










