""" Library of common functions within jobs """

# Required imports

from trident.util import general
from trident.util.notifications import afsys_send_email
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



# - Connect to a MsSql database



# - Connect to an Oracle database



# - Connect to our ftp site



# - Make a post request to an API



# - Make a get request to an API