""" Fleet _jobs file """

# Required imports

from trident.util import general
import logging
from airflow.hooks.base_hook import BaseHook

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

import cx_Oracle

# -- Imports for transformations

import pandas as pd
import numpy as np

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

#: DAG function
def get_availability():
    """ Extract availability base data """
    credentials = BaseHook.get_connection(conn_id="FLEET_FOCUS")
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

    sql= general.file_to_string('./sql/availability.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)
    df.columns = [x.lower() for x in df.columns]

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{temp_path}/fleet_avail.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

    return "Successfully queried Fleet Focus delays main table"

#: DAG function
def calc_availability():
    """ Calculate daily availability by priority
        Using availability base data extracted previously
        To match the status-based calculation in 
        Fleet's Crystal report
    """

    df = pd.read_csv(f"{temp_path}/fleet_avail.csv",low_memory=False)

    df['priority_clean'] = df['pri_shop_priority'].apply(lambda x: "1" if "1" in x else ("2" if "2" in x else np.nan))
    
    df_filtered = df.loc[(df['asset_type'] == "ASSET")&
    df['procst_proc_status'].isin(['A','AG','AH'])]
    
    down = df_filtered.loc[(~df_filtered['work_order_no'].isna())&
    ((df_filtered['delay_status_list'].isna()) | 
     (df_filtered['delay_status_list'] != 'S')
    )]

    equip_unique = df_filtered.loc[:,['eqm_equip','priority_clean']].drop_duplicates()
    down_unique = down.loc[:,['eqm_equip','priority_clean']].drop_duplicates()

    pr_totals = equip_unique.groupby(['priority_clean']).size().reset_index(name='counts')
    pr_down_totals = down_unique.groupby(['priority_clean']).size().reset_index(name='counts')

    pr_calcs = pd.concat([pr_totals,pr_down_totals],
        ignore_index=True)

    general.pos_write_csv(
        pr_calcs,
        f"{prod_path}/fleet_avail_calcs.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

    return "Successfully calculated priority availability metrics"

#: DAG function
def get_delays():
    """ Extract delays data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="FLEET_FOCUS")
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

    sql= general.file_to_string('./sql/delays-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)
    df.columns = [x.lower() for x in df.columns]

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{prod_path}/fleet_delays.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

    return "Successfully queried Fleet Focus delays main table"

#: DAG function
def get_jobs():
    """ Extract work order data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="FLEET_FOCUS")
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

    sql= general.file_to_string('./sql/jobs-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)
    df.columns = [x.lower() for x in df.columns]

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{prod_path}/fleet_jobs.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

    return "Successfully queried Fleet Focus jobs main table"

#: DAG function
def get_vehicles():
    """ Extract vehicles data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="FLEET_FOCUS")
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

    sql= general.file_to_string('./sql/vehicles-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)
    df.columns = [x.lower() for x in df.columns]

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{prod_path}/fleet_vehicles.csv",
        date_format="%Y-%m-%d %H:%M:%S"
        )

    return "Successfully queried Fleet Focus eq_main table"

def process_vehicles():
    """ Processing raw vehicles extract for valid vehicles """