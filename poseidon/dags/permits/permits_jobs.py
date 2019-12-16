"""DSD permits _jobs file."""
#import os

import datetime as dt
import pandas as pd
import time
import string
import glob
import os
import numpy as np
from trident.util import general
from trident.util.data_tests import *
import logging
import cx_Oracle
from trident.util.geospatial import spatial_join_pt

conf = general.config
clean_permits = f"{conf['temp_data_dir']}/permits_latest.csv"
bid_permits = f"{conf['temp_data_dir']}/permits_bids.csv"
cur_year = dt.datetime.now().year
final_permits = f"{conf['prod_data_dir']}/dsd_permits_{cur_year}_datasd_v1.csv"
dict_url = f"{conf['prod_data_dir']}/dsd_permits_{cur_year}_datasd_v1_dict.csv"

def get_permits_files():
    """ Get permit file from ftp site. """
    logging.info('Retrieving permits data.')
    
    wget_str = "wget -np --continue " \
        + "--user=$ftp_user " \
        + "--password='$ftp_pass' " \
        + "--directory-prefix=$temp_dir " \
        + "ftp://ftp.datasd.org/uploads/dsd/" \
        + "permits/*Panda_Extract_PermitActivities*.txt"

    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
        ftp_user=conf['ftp_datasd_user'],
        ftp_pass=conf['ftp_datasd_pass'],
        temp_dir=conf['temp_data_dir']
    )

    return command

def clean_data():
    """ Get permits file and write to df """

    filename = conf['temp_data_dir'] + "/*Panda_Extract_PermitActivities*.txt"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    df = pd.read_csv(latest_file,sep=",",encoding = "ISO-8859-1")
    df.columns = [x.lower() for x in df.columns]

    df = df.rename(columns={'job_lat':'lat_job',
        'job_lng':'lng_job',
        'job_address':'address',
        'approval_issue_dt':'date_approval_issue',
        'approval_close_dt':'date_approval_close',
        'proj_appl_date':'date_proj_appl',
        'proj_deemed_cmpl_date':'date_proj_comp'
        })

    df = df.drop(['key_date_from','key_date_to'],axis=1)

    general.pos_write_csv(
    df,
    clean_permits,
    date_format='%m/%d/%Y %I:%M:%S %p')

    return 'Successfully cleaned data.'

def join_bids():
    """ Spatially joins permits to Business Improvement Districts. """

    bids_geojson = conf['prod_data_dir'] + '/bids_datasd.geojson'
    bids_join = spatial_join_pt(clean_permits,
                             bids_geojson,
                             lat='lat_job',
                             lon='lng_job')

    bids_join = bids_join.drop(['objectid',
        'long_name',
        'status',
        'link'
        ], axis=1)

    bids_join = bids_join.rename(columns={'name':'bid_name'})

    general.pos_write_csv(
        bids_join,
        bid_permits,
        date_format='%m/%d/%Y %I:%M:%S %p')

    return 'Successfully joined permits to BIDs'

def create_prod_file():
    """ Check fields and dictionary and write prod file """

    logging.info("Reading in data")
    permits_df = pd.read_csv(bid_permits)

    logging.info("Converting fields to match correct dtypes")

    logging.info("Converting float to string columns")
    
    float_to_str_cols = ['proj_job_order','job_apn','com_plan_id']
    permits_df[float_to_str_cols] = float_to_string(permits_df[float_to_str_cols])

    logging.info("Converting integer to string columns")
    
    permits_df[['approval_id',
    'approval_type_id',
    'appr_proc_code',
    'job_id',
    'proj_id',
    'devel_id']] = permits_df[['approval_id',
    'approval_type_id',
    'appr_proc_code',
    'job_id',
    'proj_id',
    'devel_id']].fillna('').astype(str)

    logging.info("Converting datetime columns")

    datetime_cols = ['date_approval_issue',
    'date_approval_close',
    'date_proj_appl',
    'date_proj_comp'
    ]

    dt_format_orig = '%m/%d/%Y %I:%M:%S %p'

    for dtcol in datetime_cols:
        permits_df[dtcol] = pd.to_datetime(permits_df[dtcol],
            errors='coerce',
            format=dt_format_orig)


    logging.info("Getting new stats on columns")

    bbox = geo_bounds(permits_df[['lat_job','lng_job']])
    logging.info(f"Got bounding box of {bbox}")

    date_ranges = date_range(permits_df[datetime_cols],time=True)

    logging.info("Checking if date range fits within specified time frame")
    date_check_min = date_ranges['date_approval_issue']['min_date']
    date_check_max = date_ranges['date_approval_issue']['max_date']
    logging.info(date_check_min.year == cur_year)
    logging.info(date_check_max.year == cur_year)

    logging.info("Getting string unique values")

    string_lists = unique_values_list(permits_df.select_dtypes(include=['object']),limit=15)
    

    num_cols = ['appl_days',
        'valuation',
        'stories',
        'units',
        'floorareas']
    
    logging.info("Calculating number stats")
    num_stats = number_stats(permits_df[num_cols])

    # Include a check for the difference between current and new min/max

    logging.info("Reading in dictionary")
    dictionary = pd.read_csv(dict_url)

    dictionary_update = update_possible_values(dictionary,
        string_vals=string_lists,
        date_vals=date_ranges,
        geo_val=bbox,
        num_vals=num_stats)

    logging.info("Updating possible values")

    general.pos_write_csv(
    permits_df,
    final_permits,
    date_format=conf['date_format_ymd_hms'])

    general.pos_write_csv(
    dictionary_update,
    dict_url,
    date_format=conf['date_format_ymd_hms'])

    #After reading in dictionary, we will do a check to see if min and max are very much different than the last time

    return "Successfully wrote prod file and dictionary"