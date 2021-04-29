"""General.py."""
import os
import errno
import pandas as pd
import logging
import requests
import shutil
from datetime import datetime, timedelta, date
from dateutil import tz
import pendulum

import subprocess
import csv
import json

from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

def get_last_run(dag):
    last_dag_run = dag.get_last_dagrun()
    if last_dag_run is None:
        return pendulum.now()
    else:
        return last_dag_run.execution_date


def seven_days_ago():
    """Return the date seven days ago."""
    return datetime.combine(datetime.today() - timedelta(7),
                            datetime.min.time())


def today():
    """Return today's date."""
    return datetime.combine(datetime.today(), datetime.min.time())


def get_year(the_date=datetime.now()):
    """Get current year, or year for date passed."""
    return the_date.strftime("%Y")


def get_today_date(the_date=datetime.now()):
    """Return today's date (no time) as string."""
    return the_date.strftime("%Y-%m-%d")

def get_date_1wk_ago():
    """ Return last week's date as string."""
    return (date.today() - timedelta(days=6)).strftime("%d")

def get_date_3mo_ago():
    """Return 3 months ago date as string."""
    return (date.today() - timedelta(3*365/12)).isoformat()


def get_date_6mo_ago():
    """Return 6 months ago date as string."""
    return (date.today() - timedelta(6*365/12)).isoformat()


def get_FY_year(the_date=datetime.now()):
    """Return Fiscal Year based on today's date."""
    if the_date.month > 6:
        return 'FY' + str(the_date.year - 2000) + '-' + str(the_date.year -
                                                            1999)
    else:
        return 'FY' + str(the_date.year - 2001) + '-' + str(the_date.year -
                                                            2000)


def get_prev_FY_year(the_date=datetime.now()):
    """!!! Only use for traffic_counts_jobs.py."""
    return 'FY' + str(the_date.year - 2001) + '-' + str(the_date.year - 2000)


def get_FY_short(the_date=datetime.now()):
    """Return 2-digit current fiscal year as integar"""
    if the_date.month > 6:
        return the_date.year - 2000
    else:
        return the_date.year - 2001


def utc_to_pst(timestamp_str, in_fmt, out_fmt):
    """Convert UTC timestamp to Local time (PST)."""
    timestamp = datetime.strptime(timestamp_str, in_fmt)
    utc_tz = tz.gettz('UTC')
    pst_tz = tz.gettz('US/Pacific')
    timestamp = timestamp.replace(tzinfo=utc_tz)
    pst_timestamp = timestamp.astimezone(pst_tz)
    return pst_timestamp.strftime(out_fmt)


def buildConfig(env):
    """Take the current environment, generate build configuration."""
    config = {
        'env': (env or 'local').upper(),
        'default_s3_conn_id': 's3data',
        'prod_data_dir': "/data/prod",
        'temp_data_dir': "/data/temp",
    }
    return config


config = buildConfig(os.environ.get('SD_ENV'))

# https://crontab.guru/
schedule = {
    'fd_incidents' : "0 8 * * *", # daily at 8am UTC / 1am PST
    'claims_stat': "@monthly",
    'pd_ripa': None,
    'pd_cfs': "0 0 * * *", # daily at 12am UTC / 5pm PST
    'pd_col': "0 0 * * *", # daily at 12am UTC / 5pm PST
    'pd_hc': None,
    'ttcs': '0 10 * * *', # daily at 10 am UTC / 3am PST
    'indicator_bacteria_tests': "0 8 * * *", # daily at 8am UTC / 1am PST
    'chem_analytes': "0 8 * * *", # daily at 8am UTC / 1am PST
    'parking_meters': '0 19 * * *', # daily at 7pm UTC, Noon PST
    'traffic_counts': "@weekly",
    'read': "0 8 * * *", # daily at 8am UTC / 1am PST
    'dsd_approvals': "0 10 * * *", # Daily at 10a UTC / 3a PST
    'streets':"0 0,1,2,3,4,14,15,16,17,18,19,20,21,22,23 * * 1-6", # every hour, 6am to 7pm, Mon-Fri PST
    'get_it_done': "0 7 * * *", # daily at 7am UTC / 11pm PST
    'special_events': "0 8 * * *", # daily at 8am UTC / 1am PST
    'waze': "*/5 * * * *",  # every 5 minutes
    'inventory': "@monthly",  # Run 1x a month at 00:00 of the 1st day of mo
    'gis_daily': '0 6 * * *',  # daily at 6am UTC / 10pm PST
    'gis_weekly': '0 10 * * 2',  # weekly on Tuesday at 10am UTC / 2am PST
    'budget': "0 17 * 5-7 5", # weekly Fridays at 5p UTC / 10am PST
    'campaign_fin': "0 11 * * *", # daily at 11am UTC / 4am PST
    'public_art': "0 11 * * *", # daily at 11am UTC / 4am PST
    'sire': "0 8 * * 1-5", # 8am UTC / 12am PST every Mon-Fri
    'onbase': "*/5 0,1,2,3,4,15,16,17,18,19,20,21,22,23 * * 1-6", # every 5 mins, 7am to 7pm, Mon-Fri PST
    'documentum_daily' : "0 8 * * 1-5", # 8am UTC / 12am PST every Mon-Fri
    'documentum_hr_30' : "30 0,1,2,3,4,15,16,17,18,19,20,21,22,23 * * 1-6", # 30 mins past the hour, 7am to 7pm, Mon-Fri PST
    'documentum_hr_15': "15 0,1,2,3,4,15,16,17,18,19,20,21,22,23 * * 1-6", # 15 mins past the hour, 7am to 7pm, Mon-Fri PST
    'tsw_integration': '0 6 * * *',  # daily at 6am UTC / 10pm PST
    'cip': "0 8 * * *", # daily at 8am UTC / 1am PST
	'cityiq': '@daily',
    'onbase_test': '*/15 * * * *',
    'gis_tree_canopy': None,
    'parking_meter_locs': '0 19 * * *', # daily at 7pm UTC
    'sidewalks': '@monthly',
    'amcs': "0 12 * * *", # Daily at 4p UTC / 5a PST
    'ga_portal': '@monthly',
    'pv_prod':'@hourly',
    'fleet':"0 12 * * *", # Daily at 4p UTC / 5a PST
    'pd_docs':"0 8 * * *", # Daily at 8a UTC / 1a PST
}

default_date = datetime(2019, 10, 8)

start_date = {
    'fd_incidents' : default_date,
    'pd_cfs': default_date,
    'pd_col': default_date,
    'pd_hc': default_date,
    'pd_ripa': datetime(2020, 3, 5),
    'claims_stat': default_date,
    'ttcs': default_date,
    'indicator_bacteria_tests': default_date,
    'chem_analytes': datetime(2020, 11, 21),
    'parking_meters': default_date,
    'traffic_counts': default_date,
    'read': default_date,
    'dsd_approvals': default_date,
    'dsd_code_enforcement': default_date,
    'streets_sdif': default_date,
    'streets_imcat': default_date,
    'streets': default_date,
    'get_it_done': default_date,
    'gid_potholes': default_date,
    'gid_ava': default_date,
    'special_events': default_date,
    'waze': default_date,
    'inventory': default_date,
    'buffer_post_promo': default_date,
    'sonar': default_date,
    'gis_daily': default_date,
    'gis_weekly': default_date,
    'budget': default_date,
    'campaign_fin': default_date,
    'public_art': default_date,
    'sire': default_date,
    'onbase': default_date,
    'documentum_daily' : datetime(2019, 10, 29),
    'documentum_hr_30' : datetime(2019, 10, 29),
    'documentum_hr_15': datetime(2019, 10, 29),
    'tsw_integration': default_date,
    'cip': default_date,
    'cityiq': default_date,
    'onbase_test': default_date,
    'gis_tree_canopy': default_date,
    'parking_meter_locs': datetime(2019, 12, 25),
    'amcs': datetime(2020, 4, 14),
    'pv_prod': datetime(2020, 2, 26),
    'sidewalks':  default_date,
    'ga_portal': datetime(2020, 5, 19),
    'fleet': datetime(2020, 8, 26),
    'pd_docs': datetime(2021, 4, 27),
}

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': "data@sandiego.gov",
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=120)
    #'on_failure_callback': notify,
    #'on_retry_callback': notify,
    #'on_success_callback': notify
    # TODO - on failure callback can be here,
    # TODO - look into sla
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def create_path_if_not_exists(path):
    """Create path if it does not exist."""
    try:
        os.makedirs(path)
        logging.info('Created path at ' + path)
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise
    return path


def pos_write_csv(df, fname, **kwargs):
    """Write csv file, creating paths as needed, with default confs."""
    default = {
        'index': False,
        'encoding': 'utf-8',
        'doublequote': True,
        'date_format': "%Y-%m-%d",
        'quoting': csv.QUOTE_ALL
    }
    csv_args = default.copy()
    csv_args.update(kwargs)
    try:
        os.makedirs(os.path.dirname(fname))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

    df.to_csv(fname, **csv_args)

def sf_write_csv(df, fname, **kwargs):
    """ Write compressed csv for snowflake """
    fname_full = f"{config['prod_data_dir']}/{fname}_snowflake.csv.gz"
    default = {
        'index':False,
        'header':False,
        'quoting':csv.QUOTE_MINIMAL,
        'compression':'gzip',
        'doublequote':True,
        'na_rep':"NULL",
        'date_format':"%Y-%m-%d %H:%M:%S",
        'escapechar':'\\'
    }
    csv_args = default.copy()
    csv_args.update(kwargs)
    df.to_csv(fname_full, **csv_args)

def file_to_string(rel_file_path, caller=None):
    """Read a file into a string variable.  Caller is __file___."""
    if caller:
        rel_file_path = expand_rel_path(caller, rel_file_path)

    with open(rel_file_path, 'r') as ftoread:
        fstring = ftoread.read()
    return fstring


def expand_rel_path(caller, rel_path):
    """Expand a relative path."""
    return os.path.join(os.path.dirname(os.path.realpath(caller)), rel_path)


def merge_dicts(orig, update):
    new_dict = orig.copy()
    new_dict.update(update)
    return new_dict
