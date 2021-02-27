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
from urllib.parse import quote_plus

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

#: DAG function
def download_calamp_daily(**context):
    """ 
    Download files from CalAmp FTP
    """
    today = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Runs at 10pm the night before and has today's date
    yesterday = today.subtract(days=1)
    dates = [f"{today.year}{today.strftime('%m')}{today.strftime('%d')}",
    f"{yesterday.year}{yesterday.strftime('%m')}{yesterday.strftime('%d')}"]

    ftp_conn = BaseHook.get_connection(conn_id="CALAMP")
    extras = ftp_conn.extra_dejson
    password = quote_plus(extras.get('password'))
    
    for day in dates:

        logging.info(f"Looking for file for {day}")

        # MUST use curl for future needed SFTP support
        command = f"curl -o {temp_path}/calamp_{day}.csv " \
                + f"sftp://{ftp_conn.login}:{password}" \
                + f"@sftpgoweb.calamp.com/mnt/array1/SanDiego_FTP/Databases/" \
                + f"{day}.csv " \
                + f"--insecure"

        command = command.format(quote(command))
        logging.info(command)

        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
    
        if p.returncode != 0:
            logging.info(error)
            raise Exception(p.returncode)

    return dates

#: DAG function
def process_calamp_daily(**context):
    """ Reading in daily temp file and processing """

    dates = context['task_instance'].xcom_pull(dag_id="fleet_vmt",
        task_ids='get_calamp')

    today = pd.read_csv(f"{temp_path}/calamp_{dates[0]}.csv",
        low_memory=False
        )
    yesterday = pd.read_csv(f"{temp_path}/calamp_{dates[1]}.csv",
        low_memory=False
        )

    logging.info(today.shape)
    logging.info(yesterday.shape)

    # Do more stuff

    #df_homebase = vehicle_homebase(df)

    return "Process daily calamp records into data files"
