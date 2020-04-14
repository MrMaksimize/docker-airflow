"""DSD permits _jobs file."""
#import os
import pandas as pd
import string
import numpy as np
from trident.util import general
import logging
from subprocess import Popen, PIPE
from shlex import quote

conf = general.config

filelist = {'Closed projects':{
                'name':'P2K_261-Panda_Extract_DSD_Projects_Closed',
                'ext':'txt'},
            'Closed approvals since 2019':{
                'name':'P2K_261-Panda_Extract_DSD_Permits_Closed',
                'ext':'txt'},
            'Active approvals since 2003':{
                'name':'P2K_261-Panda_Extract_DSD_Permits_Active',
                'ext':'txt'},
            'Active Accela PV permits all time':{
                'name':'Accela_Active_PV',
                'ext':'xls'},
            'Closed Accela PV permits all time':{
                'name':'Accela_Closed_PV',
                'ext':'xls'},
            'All other active Accela permits all time':{
                'name':'Accela_Active_NonPV',
                'ext':'xls'},
            'All other closed Accela permits all time':{
                'name':'Accela_Closed_NonPV',
                'ext':'xls'}}

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

    files = [*filelist]

    for file in files:

        logging.info(f"Checking FTP for {filelist[file].get('name')}")

        fpath = f"{filelist[file].get('name')}_{filename}.{filelist[file].get('ext')}"

        command = f"cd {conf['temp_data_dir']} && " \
        f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
        f"-o {fpath} " \
        f"ftp://ftp.datasd.org/uploads/dsd/permits/" \
        f"{fpath} -sk"

        command = command.format(quote(command))

        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        
        if p.returncode != 0:
            logging.info(f"Error with {fpath}")
            raise Exception(p.returncode)
        else:
            logging.info(f"Found {fpath}")

    return filename

def build_pts(mode='active', **context):
    """Get PTS permits and create active and closed"""

    date_cols = ['Project Create Date',
    'Project Deemed Complete Date',
    ' Approval Create Date',
    'Approval Issue Date',
    'Approval Expiration Date',
    'Approval Close Date']

    dtypes = {'Development ID':'str',
    'Project ID':'str',
    'Job ID':'str',
    'Approval ID':'str'}

    filename = context['task_instance'].xcom_pull(dag_id="dsd_permits",
        task_ids='get_permits_files')

    logging.info("Reading in PTS files")

    if mode == 'active':

        # Currently active permits
        logging.info("Reading active permits")
        active = pd.read_csv(f"{conf['temp_data_dir']}/" \
            + f"{filelist['Active approvals since 2003'].get('name')}_" \
            + f"{filename}.{filelist['Active approvals since 2003'].get('ext')}",
            low_memory=False,
            sep=",",
            encoding="ISO-8859-1",
            parse_dates=date_cols,
            dtype=dtypes)
        # Need to add closed projects to open approvals
        logging.info("Reading closed projects")
        closed_projects = pd.read_csv(f"{conf['temp_data_dir']}/" \
            + f"{filelist['Closed projects'].get('name')}_{filename}." \
            + f"{filelist['Closed projects'].get('ext')}",
            low_memory=False,
            sep=",",
            encoding="ISO-8859-1",
            parse_dates=date_cols,
            dtype=dtypes)

        df = pd.concat([active,closed_projects],sort=False)

        prod_permits = f"{conf['temp_data_dir']}/permits_set1_active.csv"

    else:

        # Closed permits
        logging.info("Reading closed permits")
        df = pd.read_csv(f"{conf['temp_data_dir']}/" \
            + f"{filelist['Closed approvals since 2019'].get('name')}_{filename}." \
            + f"{filelist['Closed approvals since 2019'].get('ext')}",
            low_memory=False,
            sep=",",
            encoding="ISO-8859-1",
            parse_dates=date_cols,
            dtype=dtypes)

        prod_permits = f"{conf['temp_data_dir']}/permits_set1_closed.csv"


    logging.info("File read successfully, renaming columns")

    df.columns = [x.lower().strip().replace(' ','_').replace('-','_') for x in df.columns]

    df = df.rename(columns={'job_latitude':'lat_job',
        'job_longitude':'lng_job',
        'job_street_address':'address_job',
        'project_create_date':'date_project_create',
        'project_deemed_complete_date':'date_project_complete',
        'approval_issue_date':'date_approval_issue',
        'approval_expiration_date':'date_approval_expire',
        'approval_create_date':'date_approval_create',
        'approval_close_date':'date_approval_close'})

    logging.info("Removing certain columns")
    # Removing columns per SME

    df = df.drop(columns=['project_expiration_date','appl_days','project_expiration_code'])

    logging.info("Writing file to temp")
    general.pos_write_csv(
    df,
    prod_permits,
    date_format=conf['date_format_ymd_hms'])

    return 'Created new PTS files'

def build_accela(mode='active', **context):
    """ Get Accela permits and create open and closed """
    
    date_cols = ['project_create_date',
    'project_deemed_complete_date',
    'approval_create_date',
    'approval_issue_date',
    'approval_will_expire_date',
    'approval_close_date']

    dtypes = {'DEVELOPMENT_ID':str,
    'PROJECT_SAP_INTERNAL_ORDER':str,
    'JOB_ID':str,
    'JOB_DRAWING_NUMBER':str,
    'JOB_BC_CODE':str,
    'JOB_BC_CODE_DESCRIPTION':str,
    'APPROVAL_CATEGORY_CODE':str}

    filename = context['task_instance'].xcom_pull(dag_id="dsd_permits",
        task_ids='get_permits_files')

    if mode == 'active':
        # Read in PV and All
        
        logging.info(f"Reading active PV permits for {filename}")
        pv = pd.read_excel(f"{conf['temp_data_dir']}/" \
            + f"{filelist['Active Accela PV permits all time'].get('name')}_{filename}." \
            + f"{filelist['Active Accela PV permits all time'].get('ext')}",
            dtype=dtypes,
            na_values=' null')
        
        logging.info(f"Reading active non PV permits for {filename}")
        other = pd.read_excel(f"{conf['temp_data_dir']}/" \
            + f"{filelist['All other active Accela permits all time'].get('name')}_{filename}." \
            + f"{filelist['All other active Accela permits all time'].get('ext')}",
            dtype=dtypes,
            na_values=' null')

        prod_permits = f"{conf['temp_data_dir']}/permits_set2_active.csv"


    else:
        # Read in PV and All
        
        logging.info(f"Reading inactive PV permits for {filename}")
        pv = pd.read_excel(f"{conf['temp_data_dir']}/" \
            + f"{filelist['Closed Accela PV permits all time'].get('name')}_{filename}." \
            + f"{filelist['Closed Accela PV permits all time'].get('ext')}",
            dtype=dtypes,
            na_values=' null')
        
        logging.info(f"Reading inactive non PV permits for {filename}")
        other = pd.read_excel(f"{conf['temp_data_dir']}/" \
            + f"{filelist['All other closed Accela permits all time'].get('name')}_{filename}." \
            + f"{filelist['All other closed Accela permits all time'].get('ext')}",
            dtype=dtypes,
            na_values=' null')

        prod_permits = f"{conf['temp_data_dir']}/permits_set2_closed.csv"
    
    logging.info("File read successfully")
    
    logging.info("Concatting PV and non PV")
    df = pd.concat([pv,other],sort=False)
    
    logging.info("Fixing col names and dropping fields will all null")
    df.columns = [x.lower().strip().replace(' ','_') for x in df.columns]
    df = df.dropna(axis=1,how='all')
    
    logging.info("Converting datetime cols")
    for col in date_cols:
        df[col] = pd.to_datetime(df[col],errors='coerce')
    
    logging.info("Stripping whitespace from string cols")
    string_cols = df.select_dtypes(include='object')
    df[string_cols.columns] = df[string_cols.columns].apply(lambda x: x.str.strip())
    
    df = df.rename(columns={'pmt_job_latitude':'lat_job',
                        'pmt_job_longitude':'lng_job',
                        'pmt_job_street_address':'address_job',
                        'pmt_job_apn':'job_apn',
                        'project_create_date':'date_project_create',
                        'project_deemed_complete_date':'date_project_complete',
                        'approval_issue_date':'date_approval_issue',
                        'approval_create_date':'date_approval_create',
                        'approval_close_date':'date_approval_close',
                        'approval_will_expire_date':'date_approval_expire'
                       })

    df = df.drop(columns=['project_expiration_date','project_expiration_code'])

    logging.info("Writing file to temp")
    general.pos_write_csv(
    df,
    prod_permits,
    date_format=conf['date_format_ymd'])


    return 'Created new Accela files'

def join_bids_permits(pt_file='set1_active', **context):
    """ Spatially joins permits to Business Improvement Districts. """

    pt_path = f"{conf['temp_data_dir']}/permits_{pt_file}.csv"
    bids_geojson = f"{conf['prod_data_dir']}/bids_datasd.geojson"
    bids_join = spatial_join_pt(pt_path,
                             bids_geojson,
                             lat='lat_job',
                             lon='lng_job')

    bids_join = bids_join.drop(['objectid',
        'long_name',
        'status',
        'link'
        ], axis=1)

    bids_join = bids_join.rename(columns={'name':'bid_name'})

    bid_permits = f"{conf['prod_data_dir']}/permits_{pt_file}_datasd.csv"

    general.pos_write_csv(
        bids_join,
        bid_permits)

    return 'Successfully joined permits to BIDs'

def create_full_set():
    """
    Create a file for internal use that has all permits together
    """

    date_cols = ['date_project_create',
    'date_project_complete',
    'date_approval_issue',
    'date_approval_expire',
    'date_approval_create',
    'date_approval_close']

    dtypes = {'development_id':str,
    'project_id':str,
    'job_id':str,
    'approval_id':str,
    'job_bc_code':str
    }

    logging.info("Reading in all PTS files")
    logging.info("Reading in active")

    pts_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_active_datasd.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes
        )
    logging.info("Reading in historical")
    pts_historical = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_closed_historical_datasd.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes,
        error_bad_lines=False
        )
    logging.info("Reading in closed")
    pts_closed = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_closed_datasd.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes
        )

    pts_all = pd.concat([pts_historical,pts_closed,pts_active],sort=False)

    logging.info(f"Combined all sets for {pts_all.shape[0]} records")

    logging.info("Sorting by approval id")
    pts_all = pts_all.sort_values('approval_id')

    logging.info("Writing to csv")
    general.pos_write_csv(
        pts_all,
        f"{conf['prod_data_dir']}/dsd_permits_all_pts.csv",
        date_format=conf['date_format_ymd_hms'])

    logging.info("Reading in all Accela files")
    logging.info("Reading in Active")
    
    accela_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_active_datasd.csv",
        low_memory=False,
        parse_dates=date_cols)

    logging.info("Reading in Closed")

    accela_closed = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_closed_datasd.csv",
        low_memory=False,
        parse_dates=date_cols)

    accela_all = pd.concat([accela_active,accela_closed],sort=False)

    logging.info(f"Combined all sets for {accela_all.shape[0]} records")

    logging.info("Sorting by approval id")

    accela_all = accela_all.sort_values('approval_id')

    logging.info("Writing to csv")

    general.pos_write_csv(
        accela_all,
        f"{conf['prod_data_dir']}/dsd_permits_all_accela.csv",
        date_format=conf['date_format_ymd'])

    return "Successfully created full PTS and Accela sets"

def create_tsw_subset():
    """ 
    Create a file for TSW for conflict management
    Quartic is contact for integration

    """

    appr_types = ['Grading + Right of Way Permit',
             'Right Of Way Permit',
             'Right Of Way Permit-Const Plan',
             'Subdivision Improvement Agrmnt',
             'ROW Permit-Traffic Control',
             'Traffic Control Plan-Permit'
            ]

    usecols = ['approval_type',
    'approval_id',
    'date_approval_expire',
    'date_approval_issue',
    'project_id',
    'project_title',
    'project_scope',
    'approval_status',
    'lng_job',
    'lat_job']

    pts_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_active_datasd.csv",
        low_memory=False,
        usecols=usecols,
        parse_dates=['date_approval_expire','date_approval_issue'],
        dtype={'approval_id':str,'project_id':str}
        )
    accela_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_active_datasd.csv",
        low_memory=False,
        usecols=usecols,
        parse_dates=['date_approval_expire','date_approval_issue'],
        dtype={'approval_id':str,'project_id':str}
        )

    pts_subset = pts_active.loc[(pts_active['approval_type'].isin(appr_types)) & 
    (pts_active['approval_status'] == 'Issued'),:]

    accela_subset = accela_active.loc[(accela_active['approval_type'].isin(appr_types)) & 
    (accela_active['approval_status'] == 'Issued'),:]

    tsw_all = pd.concat([pts_subset,accela_subset],ignore_index=True,sort=False)
    df = tsw_all[usecols]

    df.columns = ['APPROVAL_TYPE',
    'APPROVAL_ID',
    'EXPIRE_DATE',
    'ISSUE_DATE',
    'PROJECT_ID',
    'PROJECT_TITLE',
    'SCOPE',
    'STATUS',
    'LONGITUDE',
    'LATITUDE'
    ]

    df = df.sort_values(['ISSUE_DATE','APPROVAL_ID'])

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/dsd_permits_row.csv")

    return 'Successfully created TSW subset'

def create_pw_sap_subset():
    """ 

    Create a list of project+approval ids 
    for populating timecard dropdowns for Public Works
    SAP support team is contact for integration

    """

    appr_types = ['Right Of Way Permit-Const Plan',
    'Construction Change - Eng.',
    'Construction Change - Building',
    'Right Of Way Permit',
    'Grading + Right of Way Permit',
    'ROW Permit-Traffic Control',
    'Traffic Control Plan-Permit']

    status_types = ['Issued','Completed']

    usecols = ['approval_type',
    'approval_status',
    'approval_id',
    'project_id',
    'project_title',
    'address_job']

    pts_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_active_datasd.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )

    pts_historical = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_closed_historical_datasd.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )

    pts_closed = pd.read_csv(f"{conf['prod_data_dir']}/permits_set1_closed_datasd.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )
    
    active_subset = pts_active.loc[(pts_active['approval_type'].isin(appr_types)) & 
    (pts_active['approval_status'].isin(status_types)),:]

    logging.info(active_subset.shape)

    historical_subset = pts_historical.loc[(pts_historical['approval_type'].isin(appr_types)) & 
    (pts_historical['approval_status'].isin(status_types)),:]

    logging.info(historical_subset.shape)

    closed_subset = pts_closed.loc[(pts_closed['approval_type'].isin(appr_types)) & 
    (pts_closed['approval_status'].isin(status_types)),:]

    logging.info(closed_subset.shape)

    pts_all = pd.concat([active_subset,historical_subset,closed_subset],ignore_index=True,sort=False)

    accela_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_active_datasd.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )

    accela_closed = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_closed_datasd.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )

    accela_active_subset = accela_active.loc[(accela_active['approval_type'].isin(appr_types)) & 
    (accela_active['approval_status'].isin(status_types)),:]

    logging.info(accela_active_subset.shape)

    accela_closed_subset = accela_closed.loc[(accela_closed['approval_type'].isin(appr_types)) & 
    (accela_closed['approval_status'].isin(status_types)),:]

    logging.info(accela_closed_subset.shape)

    accela_all = pd.concat([accela_active_subset,accela_closed_subset],ignore_index=True,sort=False)

    traffic_title = accela_all.loc[accela_all['approval_type'] == 'Traffic Control Plan-Permit',:].apply(lambda x: f"{x['approval_type']} {x['address_job'].split(',')[0]}", axis=1)

    accela_all.loc[accela_all['approval_type'] == 'Traffic Control Plan-Permit',
    'project_id'] = accela_all.loc[accela_all['approval_type'] == 'Traffic Control Plan-Permit','approval_id']

    accela_all.loc[accela_all['approval_type'] == 'Traffic Control Plan-Permit',
    'project_title'] = traffic_title

    accela_final = accela_all[['project_id','project_title']]
    pts_final = pts_all[['project_id','project_title']]

    df = pd.concat([accela_final,pts_final],ignore_index=True,sort=False)

    df.columns = ['id','title']

    df = df.sort_values('id')
    # duplicates exist because we're using Project ID, but the dataset is approvals
    df = df.drop_duplicates('id')

    # Now set character length limit
    df['id'] = df['id'].str.slice(0,15)
    df['title'] = df['title'].str.slice(0,75)

    # Drop the one test project
    df = df[df['title'] != 'TESTPROJECT']

    # Add a column that just contains the value 01
    df['type'] = '01'

    general.pos_write_csv(
        df[['type','id','title']],
        f"{conf['prod_data_dir']}/dsd_permits_public_works.csv")

    return 'Successfully created PW timecard subset'


