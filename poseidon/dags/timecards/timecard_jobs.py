""" Timecard _jobs file """

# Required imports

from trident.util import general
import logging
from airflow.models import Variable

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

import string
import re #regex

from datetime import datetime as dt
from datetime import timedelta
import time
from dateutil.parser import parse
import pendulum # This is the date library Airflow uses with context

from shapely.geometry import Point
from trident.util import geospatial
from trident.util.geospatial import geocode_address_google
from trident.util.geospatial import get_address_for_apn
from trident.util.geospatial import spatial_join_pt
import geopandas as gpd

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# Generic functions called in template dags

def get_latest_timecard(**context):
    """ 
    Download latest timecard extract from S3
    """
    exec_date = context['execution_date']

    # Relate execution date and PPE date
    # Use Wednesday after pay period ending

    
    logging.info("Running basic Python operator task")

    return "Successfully completed basic function"

def process_latest_timecard():
    """ 
    Process new records from latest timecard extract
    """
    absence_list = Variable.get("ABSENCE_LEAVE_CODES")
    overtime_list = Variable.get("OVERTIME_TIMECARD_CODES")
    light_duty_list = Variable.get("LIGHT_DUTY_TIMECARD_CODES")
    training_list = Variable.get("TRAINING_TIMECARD_CODES")
    regular_list = Variable.get("REGULAR_TIMECARD_CODES")

    
    logging.info("Running basic Python operator task")

    return "Successfully completed basic function"