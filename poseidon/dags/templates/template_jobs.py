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

def python_basic():
    """ 
    This is a function for the basic Python Operator
    """
    
    logging.info("Running basic Python operator task")

    return "Successfully completed basic function"

def python_context(**context):
    """ 
    This is a function for a Python Operator with context
    """
    
    logging.info("Running Python operator task with context")

    # Get context
    exec_date = context['execution_date']

    logging.info(f"Run date is {exec_date}")

    # Push context
    context['task_instance'].xcom_push(key='temp_check', value=True)

    return "Successfully completed context function"

def python_kwarg(mode=''):
    """ 
    This is a function for a Python Operator with kwargs
    """
    
    logging.info("Running python operator task with kwargs")

    return f"Successfully completed kwarg function for {mode}"

def python_both(mode='',**context):
    """ 
    This is a function for a Python Operator with both
    """

    logging.info("Running Python operator task with both")

    exec_date = context['execution_date']

    logging.info(f"Run date is {exec_date}")

    return f"Successfully completed both function for {mode}"

# -- Connect to a shared drive for one file

def bash_task():
    """ 
    This is a function for a Bash Operator
    """

    command = "echo 'Hello World'"

    return command

def sonar_task(**kwargs):
    """ 
    This is a function for a Sonar email task
    """
    # You might use this to filter a dataframe
    # For the time period of your calc
    range_start = kwargs['range_start']

    # Run code calculating actual number
    number_to_email = 25

    return {'value':number_to_email}

def email_task(**kwargs):
    """ 
    This is a function to send an email with information
    """

    logging.info("Compiling info for email")
    
    # Add some code to compile actual info
    comms = ['Info a','Info b','Info c']

    return {'info': comms}

def short_circuit_task(**context):
    """ 
    This is a function to send Airflow down a path
    """
    # This example sets up a condition based on 
    # Day and hour of week
    # It can only return boolean values

    currTime = context['execution_date'].in_timezone('America/Los_Angeles')
    if currTime.hour == 16:
        if currTime.day_of_week <= 4 :
            logging.info(f'Calling downstream tasks, hour is: {currTime.hour}')
            return True
        else:
            logging.info(f'Skipping downstream tasks, day is: {currTime.day_of_week}')
            return False
    else:
        logging.info(f'Skipping downstream tasks, hour is: {currTime.hour}')
        return False

def task_for_subdag(mode='',**context):
    """ 
    This is a function you can use within a subdag
    """
    logging.info("Subdag operator task with both mode and context")

    exec_date = context['execution_date']

    logging.info(f"Run date is {exec_date}")

    return f"Successfully completed both function for {mode} for subdag"

def template_check(**context):
    """
    This is another function to send Airflow down a path
    """

    # In this example, we check an output from a previous task
    # Then use that to determine which path to go down
    # It can only return other task ids

    trigger = context['task_instance'].xcom_pull(key='temp_check', task_ids='python_task_basic')
    
    if trigger:
        return "template_send_updated"
    else:
        return "python_task_both"