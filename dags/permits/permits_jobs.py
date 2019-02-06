"""DSD permits _jobs file."""
#import os
from datetime import datetime, timedelta
import pandas as pd
import time
from poseidon.util import general
import logging
import cx_Oracle

conf = general.config
credentials = general.source['dsd_permits']
year = general.get_year()
temp_permits = conf['temp_data_dir'] + '/permits_{}.csv'.format(year)
prod_permits = conf['prod_data_dir'] + '/dsd_permits_{}_datasd.csv'.format(year)
solar_permits = conf['prod_data_dir'] + '/solar_permits_{}_datasd.csv'.format(year)


def get_permits_files():
    """Query DB for 'permits' and save data to temp directory."""
    logging.info('Retrieving permits data.')
    db = cx_Oracle.connect(credentials)
    sql = general.file_to_string('./sql/pts.sql', __file__)
    sql += "WHERE a.issue_dt >= TO_DATE('"+year+"-JAN-01', 'YYYY-MON-DD')"
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

    logging.info('Subsetting solar permits')

    solar = df[df['approval_type_id'] == 293]

    general.pos_write_csv(
    solar,
    solar_permits,
    date_format=conf['date_format_ymd_hms'])

    return 'Successfully cleaned data.'