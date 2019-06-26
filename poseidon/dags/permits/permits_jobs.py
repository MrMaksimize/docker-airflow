"""DSD permits _jobs file."""
#import os
from datetime import datetime, timedelta
import pandas as pd
import time
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
    db = cx_Oracle.connect(credentials)
    sql = general.file_to_string('./sql/pts.sql', __file__)
    next_year = year+1
    sql += "WHERE a.issue_dt >= TO_DATE('"+str(year)+"-JAN-01', 'YYYY-MON-DD') AND a.issue_dt < TO_DATE('"+str(next_year)+"-JAN-01', 'YYYY-MON-DD')"
    df = pd.read_sql_query(sql, db)
    logging.info('Query returned {} results for {}'.format(df.shape[0],year))
    general.pos_write_csv(
        df,
        temp_permits,
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully retrieved permits data.'

def clean_data():
    """Get the permits file from temp directory, clean it, and save it in Prod directory"""

    df = pd.read_csv(temp_permits)
    df.columns = [x.lower() for x in df.columns]
    df['approval_issue_dt'] = pd.to_datetime(
    df['approval_issue_dt'], errors='coerce')
    df['approval_close_dt'] = pd.to_datetime(
    df['approval_close_dt'], errors='coerce')

    df['proj_appl_date'] = pd.to_datetime(
    df['proj_appl_date'], errors='coerce')

    df['proj_deemed_cmpl_date'] = pd.to_datetime(
    df['proj_deemed_cmpl_date'], errors='coerce')

    df = df.sort_values(by='approval_issue_dt')

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
                             lat='job_lat',
                             lon='job_lng')

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