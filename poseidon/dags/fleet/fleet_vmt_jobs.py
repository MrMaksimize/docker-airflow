""" Fleet VMT _jobs file """

# Required imports

from trident.util import general
import logging

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

import ftplib
import subprocess
from subprocess import Popen, PIPE
import requests
import glob
import os
from shlex import quote

# -- Imports for transformations

import pandas as pd
import numpy as np
import math

from collections import OrderedDict
import json

import string
import re #regex

from datetime import datetime as dt
from datetime import timedelta
import time
from dateutil.parser import parse
import pendulum # This is the date library Airflow uses with context

from airflow.hooks.base_hook import BaseHook

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# Generic functions called in template dags

#: Helper function
def vehicle_homebase():

    # Code to calculate rows to append to homebase file

    # Read in existing homebase file

    # Append records

    # Write back out
    general.pos_write_csv(
    df,
    f"{conf['temp_data_dir']}/.csv")


#: DAG function
def download_calamp_daily(**context):
    """ 
    Download files from CalAmp FTP
    """
    today = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Runs at 10pm the night before and has today's date
    yesterday = today.subtract(days=1)
    
    # Need zero-padded month and date
    today_filename = f"{today.year}" \
    f"{today.strftime('%m')}" \
    f"{today.strftime('%d')}"

    # Need zero-padded month and date
    yesterday_filename = f"{today.year}" \
    f"{today.strftime('%m')}" \
    f"{today.strftime('%d')}"

    logging.info(f"Looking for file for {today}")
    logging.info(f"Using {today_filename} as filename")

    ftp_conn = BaseHook.get_connection(conn_id="CALAMP")
    
    # MUST use curl for future needed SFTP support
    command = f"curl -o {today_filename}.csv " \
            + "sftp://sftpgoweb.calamp.com/mnt/array1/SanDiego_FTP/Databases/"\
            + f"-u {ftp_conn.login}:{ftp_conn.password} -k"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        logging.info(error)
        raise Exception(p.returncode)
    else:
        logging.info(f"Found file for {today}")
        logging.info(f"Looking for file for {yesterday}")

        # MUST use curl for future needed SFTP support
        command = f"curl -o {yesterday_filename}.csv " \
                + "sftp://sftpgoweb.calamp.com/mnt/array1/SanDiego_FTP/Databases/"\
                + f"-u {ftp_conn.login}:{ftp_conn.password} -k"

        command = command.format(quote(command))

        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        
        if p.returncode != 0:
            logging.info(error)
            raise Exception(p.returncode)
        else:
            logging.info(f"Found file for {yesterday}")

            return f"{today}"

#: DAG function
def process_calamp_daily(**context):
    """ Reading in daily temp file and processing """

    today = context['task_instance'].xcom_pull(dag_id="fleet_vmt",
        task_ids='get_calamp')

    yesterday = today.subtract(days=1)

    # Need zero-padded month and date
    today_filename = f"{today.year}" \
    f"{today.strftime('%m')}" \
    f"{today.strftime('%d')}"

    # Need zero-padded month and date
    yesterday_filename = f"{today.year}" \
    f"{today.strftime('%m')}" \
    f"{today.strftime('%d')}"

    df_today = pd.read_csv(f"{temp_path}/{today_filename}.csv")
    df_yesterday = pd.read_csv(f"{temp_path}/{yesterday_filename}.csv")

    # Do more stuff

    df_homebase = vehicle_homebase(df)

    return "Process daily calamp records into data files"
