"""DSD permits _jobs file."""
#import os
from datetime import datetime, timedelta
import pandas as pd
import time
import string
import glob
import os
import numpy as np
from trident.util import general
import logging
import cx_Oracle
from trident.util.geospatial import spatial_join_pt

conf = general.config
prod_permits = f"{conf['temp_data_dir']}/permits_latest.csv"
bid_permits = f"{conf['prod_data_dir']}/dsd_permits_2019_datasd_v1.csv" 

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
    """Get the permits file from temp directory, clean it, and save it in Prod directory"""

    filename = conf['temp_data_dir'] + "/*Panda_Extract_PermitActivities*.txt"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    df = pd.read_table(latest_file,sep=",",encoding = "ISO-8859-1")
    df.columns = [x.lower() for x in df.columns]

    final_cols = ["approval_id",
    "approval_type_id",
    "short_desc",
    "approval_type",
    "appr_proc_code",
    "cat_code",
    "authority",
    "appl_days",
    "approval_status",
    "date_approval_issue",
    "date_approval_close",
    "job_id",
    "proj_id",
    "devel_id",
    "proj_title",
    "proj_scope",
    "proj_job_order",
    "date_proj_appl",
    "date_proj_comp",
    "lng_job",
    "lat_job",
    "job_apn",
    "address",
    "com_plan_id",
    "com_plan",
    "cust_name",
    "valuation",
    "stories",
    "units",
    "floorareas",
    "bc_group"]

    df = df.rename(columns={'job_lat':'lat_job',
        'job_lng':'lng_job',
        'job_address':'address',
        'approval_issue_dt':'date_approval_issue',
        'approval_close_dt':'date_approval_close',
        'proj_appl_date':'date_proj_appl',
        'proj_deemed_cmpl_date':'date_proj_comp'
        })


    df_final = df[final_cols]

    general.pos_write_csv(
    df_final,
    prod_permits,
    date_format=conf['date_format_ymd_hms'])

    return 'Successfully cleaned data.'

def join_bids():
    """ Spatially joins permits to Business Improvement Districts. """

    bids_geojson = conf['prod_data_dir'] + '/bids_datasd.geojson'
    bids_join = spatial_join_pt(prod_permits,
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
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return 'Successfully joined permits to BIDs'