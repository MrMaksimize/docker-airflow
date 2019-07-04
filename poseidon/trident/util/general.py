"""General.py."""
import os
import errno
import pandas as pd
import logging
import requests
import shutil
from datetime import datetime, timedelta, date
from dateutil import tz

import subprocess
import csv
import json

from airflow.models import Variable

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
        'home_dir': os.environ.get("AIRFLOW_HOME", ""),
        'date_format_ymd': "%Y-%m-%d",
        'date_format_ymd_hms': "%Y-%m-%d %H:%M:%S",
        'date_format_keen': "%Y-%m-%dT%H:%M:%S",
        'dags_dir': "{}/poseidon/dags".format(os.environ.get("AIRFLOW_HOME", "")),
        'dest_s3_bucket': os.environ.get('S3_DATA_BUCKET', 'datasd-dev'),
        'ref_s3_bucket': os.environ.get('S3_REF_BUCKET', 'datasd-reference'),
        'oracle_wpl': os.environ.get('CONN_ORACLEWPL'),
        'ftp_sannet_user': os.environ.get("FTP_SANNET_USER", "anonymous"),
        'ftp_sannet_pass': os.environ.get("FTP_SANNET_PASS", "anonymous"),
        'ftp_datasd_user': os.environ.get("FTP_DATASD_USER"),
        'ftp_datasd_pass': os.environ.get("FTP_DATASD_PASS"),
        'ftp_read_user': os.environ.get("FTP_READ_USER"),
        'ftp_read_pass': os.environ.get("FTP_READ_PASS"),
        'mrm_sannet_user': os.environ.get("MRM_SANNET_USER"),
        'mrm_sannet_pass': os.environ.get("MRM_SANNET_PASS"),
        'alb_sannet_user': os.environ.get("ALB_SANNET_USER"),
        'alb_sannet_pass': os.environ.get("ALB_SANNET_PASS"),
        'mrm_sf_user': os.environ.get("MRM_SF_USER"),
        'mrm_sf_pass': os.environ.get("MRM_SF_PASS"),
        'mrm_sf_token': os.environ.get("MRM_SF_TOKEN"),
        'gh_tokens': os.environ.get("GH_TOKENS").split(','),
        'mail_notify': int(os.environ.get("MAIL_NOTIFY")),
        'mail_from_name': os.environ.get("MAIL_FROM_NAME"),
        'mail_from_addr': os.environ.get("MAIL_FROM_ADDR"),
        'mail_from_reply_to': os.environ.get("MAIL_FROM_REPLY_TO"),
        'mail_default_receivers': os.environ.get("MAIL_DEFAULT_RECEIVERS"),
        'mail_swu_key': os.environ.get("MAIL_SWU_KEY"),
        'mail_swu_sys_tpl': os.environ.get("MAIL_SWU_SYS_TPL"),
        'mail_swu_file_updated_tpl':
        os.environ.get("MAIL_SWU_FILE_UPDATED_TPL"),
        'keen_notify': int(os.environ.get("KEEN_NOTIFY")),
        'keen_project_id': os.environ.get('KEEN_PROJECT_ID'),
        'keen_write_key': os.environ.get('KEEN_WRITE_KEY'),
        'keen_read_key': os.environ.get('KEEN_READ_KEY'),
        'keen_ti_collection': os.environ.get('KEEN_TI_COLLECTION'),
        'mrm_buffer_access_token': os.environ.get('MRM_BUFFER_ACCESS_TOKEN'),
        'executable_path': f"{os.environ.get('AIRFLOW_HOME')}/poseidon/bin",
        'google_token': os.environ.get("GOOGLE_TOKEN"),
        'sde_user': os.environ.get("SDE_USER"),
        'sde_pw': os.environ.get("SDE_PW"),
        'sde_server': os.environ.get("SDE_SERVER"),
        'shiny_acct_name': os.environ.get("SHINY_ACCT_NAME"),
        'shiny_token': os.environ.get("SHINY_TOKEN"),
        'shiny_secret': os.environ.get("SHINY_SECRET")
    }
    return config


config = buildConfig(os.environ.get('SD_ENV'))

# https://crontab.guru/
schedule = {
    'fd_incidents' : "@daily",
    'claims_stat': "@daily",
    'pd_cfs': "@daily",
    'pd_col': "@daily",
    'ttcs': "@daily",
    'indicator_bacteria_tests': "@daily",
    'parking_meters': "@daily",
    'traffic_counts': "@weekly",
    'read': "@daily",
    'dsd_approvals': "@daily",
    'dsd_code_enforcement': "@daily",
    'streets_sdif': "@daily",
    'streets_imcat': "@daily",
    'get_it_done': "@hourly",
    'gid_potholes': "0 12 * * *",
    'gid_ava': "0 12 * * *",
    'special_events': "@daily",
    'waze': "*/5 * * * *",  # every 5 minutes
    'inventory': "@monthly",  # Run 1x a month at 00:00 of the 1st day of mo
    'buffer_post_promo': "@daily",
    'sonar': '@daily',
    'gis_daily': '0 6 * * *',  # daily at 6am UTC / 10pm PST
    'gis_weekly': '0 10 * * 2',  # weekly on Tuesday at 10am UTC / 2am PST
    'budget': "@weekly",
    'campaign_fin': "@daily",
    'public_art': '@daily',
    'sire': "0 7 * * *",
    'onbase': "*/5 * * * *",
    'documentum_24' : "0 7 * * *",
    'documentum_others' : "30 * * * *",
    'tsw_integration': '0 6 * * *',  # daily at 6am UTC / 10pm PST
    'cip': '@daily',
    'onbase_test': None,
    'gis_tree_canopy': None
}

default_date = datetime(2019, 6, 21)

start_date = {
    'fd_incidents' : default_date,
    'pd_cfs': default_date,
    'pd_col': default_date,
    'claims_stat': default_date,
    'ttcs': default_date,
    'indicator_bacteria_tests': default_date,
    'parking_meters': default_date,
    'traffic_counts': default_date,
    'read': default_date,
    'dsd_approvals': default_date,
    'dsd_code_enforcement': default_date,
    'streets_sdif': default_date,
    'streets_imcat': default_date,
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
    'documentum_24' : default_date,
    'documentum_others' : default_date,
    'tsw_integration': default_date,
    'cip': default_date,
    'onbase_test': datetime(2019, 6, 25),
    'gis_tree_canopy': datetime(2019, 6, 30)
}


source = {'ttcs': os.environ.get('CONN_ORACLETTCS'),
'cef':os.environ.get('CONN_ORACLE_CEF'),
'dsd_permits' : os.environ.get('CONN_ORACLE_PERMITS'),
'cip': os.environ.get('CONN_ORACLECIP'),
'risk': os.environ.get('CONN_ORACLE_RISK')
}

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': config['mail_default_receivers'],
    'email_on_failure': config['mail_notify'],
    'email_on_retry': False,
    'retries': 3,
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
        'date_format': config['date_format_ymd'],
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
