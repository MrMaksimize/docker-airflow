"""DSD permits _jobs file."""
#import os
import pandas as pd
import string
import numpy as np
from trident.util import general
from trident.util.geospatial import spatial_join_pt
import logging
from subprocess import Popen, PIPE
from shlex import quote
from datetime import datetime as dt
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import cx_Oracle

conf = general.config

filelist = {'Closed PTS projects':{
                'name':'04_dsd_projclosed',
                'ext':None},
            'Closed PTS approvals since 2019':{
                'name':'03_dsd_permitsclosed',
                'ext':None},
            'Active PTS approvals since 2003':{
                'name':'02_dsd_permitsactive',
                'ext':None},
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

def get_accela_files(**context):
    """ Get permit file from ftp site. """
    logging.info('Retrieving Accela data.')

    exec_date = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Runs on Monday for data extracted Sunday
    file_date_1 = exec_date.subtract(days=1)

    # Need zero-padded month and date
    filename_1 = f"{file_date_1.year}" \
    f"{file_date_1.strftime('%m')}" \
    f"{file_date_1.strftime('%d')}"

    file_date_2 = exec_date

    # Need zero-padded month and date
    filename_2 = f"{file_date_2.year}" \
    f"{file_date_2.strftime('%m')}" \
    f"{file_date_2.strftime('%d')}"

    files = [*filelist]

    conn = BaseHook.get_connection(conn_id="SVC_ACCT")

    for file in files:

        if 'accela' in file.lower():

            logging.info(f"Checking for {filelist[file].get('name')}")

            fpath = f"{filelist[file].get('name')}_{filename_1}.{filelist[file].get('ext')}"

            command = "smbclient //ad.sannet.gov/dfs " \
            + f"--user={conn.login}%{conn.password} -W ad -c " \
            + "'prompt OFF;"\
            + " cd \"DSD-Shared/All_DSD/Panda/\";" \
            + " lcd \"/data/temp/\";" \
            + f" get {fpath};'"

            command = command.format(quote(command))

            p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate()
            
            if p.returncode != 0:

                logging.info(f"Error with {fpath}")

                logging.info(f"Checking for {filelist[file].get('name')}")

                fpath = f"{filelist[file].get('name')}_{filename_2}.{filelist[file].get('ext')}"

                command = "smbclient //ad.sannet.gov/dfs " \
                + f"--user={conn.login}%{conn.password} -W ad -c " \
                + "'prompt OFF;"\
                + " cd \"DSD-Shared/All_DSD/Panda/\";" \
                + " lcd \"/data/temp/\";" \
                + f" get {fpath};'"

                command = command.format(quote(command))

                p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
                output, error = p.communicate()
                
                if p.returncode != 0:

                    logging.info(f"Error with {fpath}")
                    logging.info("Could not find files for either day")
                    logging.info(output)
                    logging.info(error)
                    raise Exception(p.returncode)

                else:

                    logging.info(f"Found {fpath}")
                    filedate_final = filename_2
            else:

                logging.info(f"Found {fpath}")
                filedate_final = filename_1

    return filedate_final

def get_pts_views():
    """ Get permit file from ftp site. """

    logging.info('Retrieving data from Oracle database')
    # This requires that otherwise optional credentials variable
    
    credentials = BaseHook.get_connection(conn_id="DSD_PTS")
    
    conn_config = {
            'user': credentials.login,
            'password': credentials.password
        }
    
    dsn = credentials.extra_dejson.get('dsn', None)
    sid = credentials.extra_dejson.get('sid', None)
    port = credentials.port if credentials.port else 1521
    conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, sid)

    db = cx_Oracle.connect(conn_config['user'],
        conn_config['password'],
        conn_config['dsn'],
        encoding="UTF-8")

    # OracleHook will not work
    #db = OracleHook.get_connection("RISK")

    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    files = [*filelist]
    for file in files:

        if 'pts' in file.lower():
            fpath = filelist[file].get('name')
            logging.info(f"Checking for {filelist[file].get('name')}")
            sql= general.file_to_string(f'./sql/{fpath}.sql', __file__)
            df = pd.read_sql_query(sql, db)
            logging.info(f'Query returned {df.shape[0]} results')

            general.pos_write_csv(
                df,
                f"{conf['temp_data_dir']}/{fpath}.csv")

    return 'Successfully retrieved Oracle data.'    


def build_pts(**context):
    """Get PTS permits and create active and closed"""

    date_cols = ['Project Create Date',
    'Project Deemed Complete Date',
    'Approval Create Date',
    'Approval Issue Date',
    'Approval Expiration Date',
    'Approval Close Date']

    dtypes = {'Development ID':'str',
    'Project ID':'str',
    'Job ID':'str',
    'Approval ID':'str'}

    exec_date = context['next_execution_date'].in_tz(tz='US/Pacific')
    old_file_date = exec_date.subtract(days=1)
    new_file_date = exec_date

    # Get zero-padded month and date
    old_filename = f"{old_file_date.year}" \
    f"{old_file_date.strftime('%m')}" \
    f"{old_file_date.strftime('%d')}"

    # Get zero-padded month and date
    new_filename = f"{new_file_date.year}" \
    f"{new_file_date.strftime('%m')}" \
    f"{new_file_date.strftime('%d')}"
    
    logging.info(f"Reading active permits {new_filename}")
    active = pd.read_csv(f"{conf['temp_data_dir']}/" \
        + f"{filelist['Active PTS approvals since 2003'].get('name')}.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes)    

    # Closed permits
    logging.info(f"Reading closed permits {new_filename}")
    closed = pd.read_csv(f"{conf['temp_data_dir']}/" \
        + f"{filelist['Closed PTS approvals since 2019'].get('name')}.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes)

    # Closed projects, open approvals
    logging.info(f"Reading closed projects {new_filename}")
    closed_pr = pd.read_csv(f"{conf['temp_data_dir']}/" \
        + f"{filelist['Closed PTS projects'].get('name')}.csv",
        low_memory=False,
        parse_dates=date_cols,
        dtype=dtypes)

    logging.info("Files read successfully, concatting")

    df_new = pd.concat([active,closed,closed_pr],sort=True,ignore_index=True)

    df_new['file_date'] = new_filename

    df_new.columns = [x.lower().strip().replace(' ','_').replace('-','_') for x in df_new.columns]

    df_new = df_new.rename(columns={'job_latitude':'lat_job',
        'job_longitude':'lng_job',
        'job_street_address':'address_job',
        'project_create_date':'date_project_create',
        'project_deemed_complete_date':'date_project_complete',
        'approval_issue_date':'date_approval_issue',
        'approval_expiration_date':'date_approval_expire',
        'approval_create_date':'date_approval_create',
        'approval_close_date':'date_approval_close'})

    logging.info("Reading in existing")

    df_old = pd.read_csv(f"{conf['temp_data_dir']}/dsd_permits_all_pts.csv",
        low_memory=False,
        dtype={'approval_id':str})

    prod_cols = df_old.columns.tolist()

    df_old['file_date'] = old_filename

    all_records = pd.concat([df_new,df_old],
        sort=True,
        ignore_index=True)
    
    logging.info(f"New files contain {df_new.shape[0]} records")
    logging.info(f"Old file contains {df_old.shape[0]} records")
    logging.info(f"Combined is {all_records.shape[0]} records")

    all_sorted = all_records.sort_values(['approval_id','file_date'],ascending=[True,False])
    logging.info(f"All sorted has {all_sorted.shape[0]} records")
    deduped = all_sorted.drop_duplicates(subset='approval_id')

    logging.info(f"Deduped file has {deduped.shape[0]} records")

    logging.info("Writing file to temp")

    general.pos_write_csv(
    deduped[prod_cols],
    f"{conf['temp_data_dir']}/dsd_permits_all_pts.csv",
    date_format="%Y-%m-%d %H:%M:%S")

    return 'Created new PTS file'

def build_accela(**context):
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

    filename = context['task_instance'].xcom_pull(dag_id="dsd_permits.get_create_accela",
        task_ids='get_accela_files')

    #filename = "20201116"
        
    logging.info(f"Reading active PV permits for {filename}")
    pv_active = pd.read_excel(f"{conf['temp_data_dir']}/" \
        + f"{filelist['Active Accela PV permits all time'].get('name')}_{filename}." \
        + f"{filelist['Active Accela PV permits all time'].get('ext')}",
        dtype=dtypes,
        na_values=' null')

    logging.info(f"Reading active non PV permits for {filename}")
    other_active = pd.read_excel(f"{conf['temp_data_dir']}/" \
        + f"{filelist['All other active Accela permits all time'].get('name')}_{filename}." \
        + f"{filelist['All other active Accela permits all time'].get('ext')}",
        dtype=dtypes,
        na_values=' null')
        
    logging.info(f"Reading inactive PV permits for {filename}")
    pv_closed = pd.read_excel(f"{conf['temp_data_dir']}/" \
        + f"{filelist['Closed Accela PV permits all time'].get('name')}_{filename}." \
        + f"{filelist['Closed Accela PV permits all time'].get('ext')}",
        dtype=dtypes,
        na_values=' null')
    
    logging.info(f"Reading inactive non PV permits for {filename}")
    other_closed = pd.read_excel(f"{conf['temp_data_dir']}/" \
        + f"{filelist['All other closed Accela permits all time'].get('name')}_{filename}." \
        + f"{filelist['All other closed Accela permits all time'].get('ext')}",
        dtype=dtypes,
        na_values=' null')
    
    logging.info("Files read successfully")
    
    logging.info("Concatting all")
    df = pd.concat([pv_active,pv_closed,other_active,other_closed],
        sort=True,
        ignore_index=True)
    
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

    df['file_date'] = filename

    logging.info("Reading in existing file")

    df_old = pd.read_csv(f"{conf['temp_data_dir']}/dsd_permits_all_accela.csv",
        low_memory=False,
        dtype={'approval_id':str}
        )

    prod_cols = df_old.columns.tolist()

    prev_exec_date = context['execution_date'].in_tz(tz='US/Pacific')
    old_file_date = prev_exec_date.subtract(days=1)

    # Need zero-padded month and date
    old_filename = f"{old_file_date.year}" \
    f"{old_file_date.strftime('%m')}" \
    f"{old_file_date.strftime('%d')}"

    #old_filename = "20201102"

    df_old['file_date'] = old_filename

    all_records = pd.concat([df,df_old],
        sort=True,
        ignore_index=True)
    
    logging.info(f"New files contain {df.shape[0]} records")
    logging.info(f"Old file contains {df_old.shape[0]} records")
    logging.info(f"Combined is {all_records.shape[0]} records")

    all_sorted = all_records.sort_values(['approval_id','file_date'],ascending=[True,False])
    deduped = all_sorted.drop_duplicates(subset='approval_id')

    logging.info(f"Deduped file has {deduped.shape[0]} records")

    logging.info("Writing file to temp")

    general.pos_write_csv(
    deduped[prod_cols],
    f"{conf['temp_data_dir']}/dsd_permits_all_accela.csv",
    date_format="%Y-%m-%d")

    return 'Created new Accela files'

def spatial_joins(pt_file='',**context):
    """ Spatially joins permits to Business Improvement Districts. """

    pt_path = f"{conf['temp_data_dir']}/{pt_file}.csv"

    logging.info("Reading in point file")
    point = pd.read_csv(pt_path,
        low_memory=False,
        dtype={'approval_id':str}
        )

    logging.info(f"{pt_path} has {point.shape[0]} records")
    point_cols = point.columns.tolist()
    prod_cols = point_cols + ['bid_name','council_district','zip']

    bucket_name=Variable.get('S3_REF_BUCKET')
    s3_url = f"s3://{bucket_name}/reference/{pt_file}_polygons.csv"
    ref_df = pd.read_csv(s3_url,
        low_memory=False,
        dtype={'approval_id':str}
        )

    logging.info("Read ref file")

    merge = pd.merge(point,ref_df,how='left',on=['approval_id'])

    logging.info(f"Merge has {merge.shape[0]} records")

    missing = merge[(merge['zip'].isna()) &
      (merge['council_district'].isna()) &
      (merge['bid_name'].isna()) &
      (~merge['lng_job'].isna()) &
      (~merge['lat_job'].isna())]

    missing_ids = missing.loc[:,'approval_id'].tolist()

    complete = merge[~merge['approval_id'].isin(missing_ids)]

    logging.info(f'Need to get polygons for {missing.shape[0]}')
    logging.info(f"Have polygons for {complete.shape[0]}")

    logging.info("Joining BIDS")

    if missing.empty:

        logging.info("Do not need any joins. Writing file")

        final = complete

    else:

        missing = missing.drop(columns=['bid_name','council_district','zip'])
        
        bids = spatial_join_pt(missing,
            f"{conf['prod_data_dir']}/bids_datasd.geojson",
            lat='lat_job',
            lon='lng_job')

        bids = bids.drop(['objectid',
            'long_name',
            'status',
            'link'
            ], axis=1)

        bids = bids.rename(columns={'name':'bid_name'})

        logging.info("Joining council districts")

        cd = spatial_join_pt(bids,
            f"{conf['prod_data_dir']}/council_districts_datasd.geojson",
            lat='lat_job',
            lon='lng_job')

        cd = cd.drop(['objectid',
            'name',
            'phone',
            'website',
            'perimeter',
            'area'
            ],axis=1)

        cd = cd.rename(columns={'district':'council_district'})

        logging.info("Joining ZIPS")

        zips = spatial_join_pt(cd,
            f"{conf['prod_data_dir']}/zip_codes_datasd.geojson",
            lat='lat_job',
            lon='lng_job')

        zips = zips.drop(['objectid',
            'community'], axis=1)

        new_polygon_rows = zips[['approval_id','bid_name','council_district','zip']]

        final_polygons = pd.concat([ref_df,new_polygon_rows],ignore_index=True,sort=False)

        final_polygons = final_polygons.drop_duplicates()

        general.pos_write_csv(
            final_polygons,
            f"{conf['prod_data_dir']}/{pt_file}_polygons.csv")

        final = pd.concat([complete,zips],ignore_index=True,sort=True)

    general.pos_write_csv(
        final[prod_cols],
        f"{conf['prod_data_dir']}/{pt_file}.csv")

    return f'Successfully joined permits to polygons'

def create_subsets(mode='set1',**context):
    """
    Create subsets for public use
    """

    date_cols = ['date_project_create',
    'date_project_complete',
    'date_approval_issue',
    'date_approval_create',
    'date_approval_close',
    'date_approval_expire'
    ]

    dtypes = {'development_id':str,
    'project_id':str,
    'job_id':str,
    'approval_id':str,
    'job_bc_code':str
    }


    logging.info(f"Reading in {mode}")

    if mode == 'set1':
        filepath = "dsd_permits_all_pts.csv"
        comp_csv_path = 'dsd_approvals_pts'
    elif mode == 'set2':
        filepath = "dsd_permits_all_accela.csv"
        comp_csv_path = 'dsd_approvals_accela'
    else:
        raise Exception('Invalid mode')
    
    df = pd.read_csv(f"{conf['prod_data_dir']}/{filepath}",
        low_memory=False)
        #parse_dates=date_cols)

    logging.info(f"File has {df.shape[0]} records")

    logging.info("Converting datetime cols")
    for col in date_cols:
        df[col] = pd.to_datetime(df[col],errors='coerce')

    logging.info(f"Writing compressed csv")
    general.sf_write_csv(df,comp_csv_path)

    closed = df.loc[~df['date_approval_close'].isna()]

    if mode == 'set1':
        active = df.loc[(df['date_approval_close'].isna()) & (df['project_status'] != 'Closed')]
    elif mode == 'set2':
        active = df.loc[df['date_approval_close'].isna()]
    else:
        raise Exception('Invalid mode')

    logging.info(f"{closed.shape[0]} records meet closed criteria")
    logging.info(f"{active.shape[0]} records meet active criteria")

    logging.info("Writing to csv")

    general.pos_write_csv(
        closed,
        f"{conf['prod_data_dir']}/permits_{mode}_closed_datasd.csv",
        date_format="%Y-%m-%d")

    general.pos_write_csv(
        active,
        f"{conf['prod_data_dir']}/permits_{mode}_active_datasd.csv",
        date_format="%Y-%m-%d")

    return f"Successfully created {mode} subsets"

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
             'Traffic Control Plan-Permit',
             'Traffic Control Permit'
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
    logging.info(f"Have {pts_active.shape[0]} active PTS permits")

    accela_active = pd.read_csv(f"{conf['prod_data_dir']}/permits_set2_active_datasd.csv",
        low_memory=False,
        usecols=usecols,
        parse_dates=['date_approval_expire','date_approval_issue'],
        dtype={'approval_id':str,'project_id':str}
        )
    logging.info(f"Have {accela_active.shape[0]} active Accela permits")

    pts_subset = pts_active.loc[(pts_active['approval_type'].isin(appr_types)) & 
    (pts_active['approval_status'] == 'Issued'),:]
    logging.info(f"PTS subset has {pts_subset.shape[0]} records")

    accela_subset = accela_active.loc[(accela_active['approval_type'].isin(appr_types)) & 
    (accela_active['approval_status'] == 'Issued'),:]
    logging.info(f"Accela subset has {accela_subset.shape[0]} records")

    test = accela_subset.loc[accela_subset['approval_id'] == 'PMT-3001698']

    if test.empty:
        logging.info("Accela subset does not contain test permit")
    else:
        logging.info("Accela subset does contain test permit")

    tsw_all = pd.concat([pts_subset,accela_subset],ignore_index=True,sort=False)

    logging.info(f"Concat has {tsw_all.shape[0]} records")

    df = tsw_all[usecols]
    logging.info(f"Final df has {df.shape[0]} records")

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
        f"{conf['prod_data_dir']}/dsd_permits_row.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

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
    'ROW Permit-Traffic Control',
    'Traffic Control Plan-Permit',
    'Traffic Control Permit'
    ]

    status_types = ['Issued','Completed','Created']

    usecols = ['approval_type',
    'approval_status',
    'approval_id',
    'project_id',
    'project_title',
    'address_job']

    pts = pd.read_csv(f"{conf['prod_data_dir']}/dsd_permits_all_pts.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )
    
    pts_all = pts.loc[(pts['approval_type'].isin(appr_types)) & 
    (pts['approval_status'].isin(status_types)),:]

    logging.info(pts_all.shape)

    accela = pd.read_csv(f"{conf['prod_data_dir']}/dsd_permits_all_accela.csv",
        low_memory=False,
        usecols=usecols,
        dtype={'approval_id':str,'project_id':str}
        )

    accela_all = accela.loc[(accela['approval_type'].isin(appr_types)) & 
    (accela['approval_status'].isin(status_types)),:]

    logging.info(accela_all.shape)

    traffic_title = accela_all.loc[accela_all['approval_type'] == 'Traffic Control Plan-Permit',:].apply(lambda x: f"{x['approval_type']} {x['address_job'].split(',')[0]}", axis=1)

    accela_all.loc[accela_all['approval_type'].isin(['Traffic Control Plan-Permit',
        'Traffic Control Permit']),
    'project_id'] = accela_all.loc[accela_all['approval_type'].isin(['Traffic Control Plan-Permit',
        'Traffic Control Permit']),'approval_id']

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


