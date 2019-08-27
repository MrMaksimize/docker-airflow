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
credentials = general.source['dsd_permits']
year = datetime.now().year
temp_permits = conf['temp_data_dir'] + '/permits_{}_extract.csv'.format(year)
prod_permits = conf['temp_data_dir'] + '/permits_{}_clean.csv'.format(year)
solar_permits = conf['prod_data_dir'] + '/solar_permits_{}_datasd_v1.csv'.format(year)
bid_permits = conf['prod_data_dir'] + '/dsd_permits_{}_datasd_v1.csv'.format(year)


def get_permits_files():
    """Query DB for 'permits' and save data to temp directory."""
    logging.info('Retrieving permits data.')
    
    wget_str = "wget -np --continue " \
     + "--user=$ftp_user " \
     + "--password='$ftp_pass' " \
     + "--directory-prefix=$temp_dir " \
     + "ftp://ftp.datasd.org/uploads/dsd/permits/pts_issued_*.csv"
    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
    ftp_user=conf['ftp_datasd_user'],
    ftp_pass=conf['ftp_datasd_pass'],
    temp_dir=conf['temp_data_dir'])

    return command

def clean_data():
    """Get the permits file from temp directory, clean it, and save it in Prod directory"""

    filename = conf['temp_data_dir'] + "/pts_issued_YTD*.csv"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    date_cols = ['APPROVAL_ISSUE_DT',
    'APPROVAL_CLOSE_DT',
    'PROJ_APPL_DATE',
    'PROJ_DEEMED_CMPL_DATE'
    ]

    df = pd.read_csv(latest_file,encoding = "ISO-8859-1",dtype={'JOB_LNG':np.float64,
        'JOB_LAT':np.float64,
        'JOB_APN':str},parse_dates=date_cols)
    
    df.columns = [x.lower() for x in df.columns]
    
    df = df.rename(columns={
        'approval_issue_dt':'date_approval_issued',
        'approval_close_dt':'date_approval_closed',
        'proj_appl_date':'date_proj_appl',
        'proj_deemed_cmpl_date':'date_proj_compl',
        'job_lng':'lng_job',
        'job_lat':'lat_job',
        'job_address':'address_job'
        })

    df = df.sort_values(by='date_approval_issued')

    logging.info('Writing all permits')

    general.pos_write_csv(
    df,
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

def subset_solar():
    """ Creating subset of solar permits """

    df = pd.read_csv(bid_permits)
    solar = df[df['approval_type_id'] == 293]

    general.pos_write_csv(
    solar,
    solar_permits,
    date_format=conf['date_format_ymd_hms'])

    return "Successfully subsetted solar permits" 