""" Library of common functions within jobs """

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
from shlex import quote
import pymssql

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

from datetime import datetime as dt
from datetime import timedelta
import time
from dateutil.parser import parse
import pendulum # This is the date library Airflow uses

from shapely.geometry import Point
from trident.util import geospatial
from trident.util.geospatial import geocode_address_google
from trident.util.geospatial import get_address_for_apn
from trident.util.geospatial import spatial_join_pt
import geopandas as gpd

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']


# -- Connect to a shared drive for multiple files

def get_smb_files():
    """Get files from shared drive with wildcard."""

    # Uses smbclient, subprocess, and shlex
    logging.info('Retrieving files')
    
    command = "smbclient //ad.sannet.gov/dfs " \
        + f"--user={conf['svc_acct_user']}%{conf['svc_acct_pass']} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"PATH/TO/FILE/\";" \
        + " lcd \"/data/temp/\";" \
        + f" mget *wildcard*.xlsx;'"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        # Breaks job
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return "Downloaded files"

# -- Connect to a shared drive for a specific file

def get_smb_file():
    """Get known file from shared drive """

    # Uses subprocess, and shlex
    logging.info('Retrieving files')
    
    command = "smbclient //ad.sannet.gov/dfs " \
        + f"--user={conf['svc_acct_user']}%{conf['svc_acct_pass']} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"PATH/TO/FILE/\";" \
        + " lcd \"/data/temp/\";" \
        + f" get specific_file.xlsx {conf['temp_data_dir']}/specific_file.xlsx;'"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        # Breaks job
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return "Downloaded file"

# - Connect to a MsSql database

def get_mssql_query():
    """ Extract data from MS SQL database """

    # This requires a folder named 'sql'
    # With a .sql file inside
    query = general.file_to_string('./sql/query.sql', __file__)
    
    # This is an environment variable
    # AIRFLOW_CONN_DUMMY_NAME
    conn = MsSqlHook(mssql_conn_id='dummy_name')

    logging.info("Read data to panda DataFrame")
    
    # This pulls in query results as DF
    df = conn.get_pandas_df(query)
        
    return "Successfully queried MS SQL data source"

# - Connect to an Oracle database

def get_oracle_data():
    """ Extract data from Oracle database """

    # This requires an environment variable
    # CONN_ORACLE_DUMMY_NAME
    # Pull environment variable into source object
    # in trident.util.general.source

    credentials = general.source['dummy_name']
    db = cx_Oracle.connect(credentials)

    # This also requires a folder named 'sql'
    # With a .sql file inside
    sql= general.file_to_string('./sql/claimstat_tsw.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)

    return "Successfully queried Oracle data source"

# - Connect to our ftp site

def get_ftp_file(**context):
    """ Downloading data from DataSD FTP """

    # For daily data dumps, use execution date 
    # Download just the most recent file
    # To make errors easier to catch

    # Operator must provide context
    exec_date = context['execution_date']
    
    # Exec date returns a Pendulum object
    file_date = exec_date.subtract(days=1)

    # Choose one OR the other of the following
    # two methods for filename

    # Need zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.strftime('%m')}" \
    f"{file_date.strftime('%d')}"

    # Need NON zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.month}" \
    f"{file_date.day}"

    fpath = f"DummyFile_{filename}.csv"

    logging.info(f"Checking FTP for {filename}")

    # MUST use curl for future needed SFTP support
    command = f"cd {conf['temp_data_dir']} && " \
    f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
    f"-o {fpath} " \
    f"ftp://ftp.datasd.org/uploads/IPS/" \
    f"{fpath} -sk"

    # Must use quote for security
    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        # Break job
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return filename