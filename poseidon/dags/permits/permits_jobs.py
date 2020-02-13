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

filelist = [{name:'P2K_261-Panda_Extract_DSD_Projects_Closed',
                ext:'txt',
                desc:'Closed projects'},
            {name:'P2K_261-Panda_Extract_DSD_Permits_Closed',
                ext:'txt',
                desc:'Closed approvals since 2019'},
            {name:'P2K_261-Panda_Extract_DSD_Permits_Active',
                ext:'txt',
                desc:'Active approvals since 2003'},
            {name:'Accela_Active_PV',
                ext:'xls',
                desc:'Active Accela PV permits all time'},
            {name:'Accela_Closed_PV',
                ext:'xls',
                desc:'Closed Accela PV permits all time'},
            {name:'Accela_Active_NonPV',
                ext:'xls',
                desc:'All other active Accela permits all time'},
            {name:'Accela_Closed_NonPV',
                ext:'xls',
                desc:'All other closed Accela permits all time'}]

def get_permits_files(**context):
    """ Get permit file from ftp site. """
    logging.info('Retrieving permits data.')

    exec_date = context['execution_date']
    # Exec date returns a Pendulum object
    file_date = exec_date.subtract(days=1)

    # Need zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.strftime('%m')}" \
    f"{file_date.strftime('%d')}"

    for file in filelist:

        logging.info(f"Checking FTP for {file}")

        fpath = f"{file['name']}_{filename}.{file['ext']}"

        command = f"cd {conf['temp_data_dir']} && " \
        f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
        f"-o {fpath} " \
        f"ftp://ftp.datasd.org/uploads/dsd/permits/" \
        f"{fpath} -sk"

        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        
        if p.returncode != 0:
            logging.info(f"Error with {fpath}")
            raise Exception(p.returncode)
        else:
            logging.info(f"Found {fpath}")
            return filename

    return filename

def build_pts(mode='active', **context):
    """Get PTS permits and create active and closed"""

    date_cols = ['Project Create Date',
    'Project Deemed Complete Date',
    'Project Expiration Date',
    ' Approval Create Date',
    'Approval Issue Date',
    'Approval Expiration Date',
    'Approval Close Date']

    dtypes = {'Development ID':'str',
    'Project ID':'str',
    'Job ID':'str',
    'Approval ID':'str',
    'Appl_Days':np.int64}

    filename = context['task_instance'].xcom_pull(task_ids='get_permits_files')

    logging.info("Reading in PTS files")

    if mode == 'active':

        # Currently active permits
        logging.info("Reading active permits")
        df = pd.read_csv(f"{filelist[2]['name']}_{filename}.{filelist[2]['ext']}",
            low_memory=False,
            sep=",",
            encoding = "ISO-8859-1",
            parse_dates=date_cols,
            dtype=dtypes)
        # Need to check active approvals against closed projects
        # Usually don't find an overlap
        logging.info("Reading closed projects")
        closed_projects = pd.read_csv(f"{filelist[0]['name']}_{filename}.{filelist[0]['ext']}")

    else:

        # Closed permits
        logging.info("Reading closed permits")
        df = pd.read_csv(f"{filelist[1]['name']}_{filename}.{filelist[1]['ext']}",
            low_memory=False,
            sep=",",
            encoding = "ISO-8859-1",
            parse_dates=date_cols,
            dtype=dtypes)

    df.columns = [x.lower().strip().replace(' ','_').replace('-','_') for x in df.columns]

    df = df.rename(columns={'job_latitude':'lat_job',
        'job_longitude':'lng_job',
        'job_street_address':'address_job',
        'project_create_date':'date_project_create',
        'project_deemed_complete_date':'date_project_complete',
        'project_expiration_date':'date_project_expire',
        'approval_issue_date':'date_approval_issue',
        'approval_expiration_date':'date_approval_expire',
        'approval_create_date':'date_approval_create',
        'approval_close_date':'date_approval_close'})

    if mode == 'active':

    df = pd.read_csv()
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

    return 'Created new PTS files'

def build_accela():
    """ Get Accela permits and create open and closed """
    filename = context['task_instance'].xcom_pull(task_ids='get_permits_files')


    return 'Created new Accela files'

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