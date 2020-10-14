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

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

def get_delays():
    """ Extract delays data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="oracle_fleet")
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
        date_format=conf['date_format_ymd_hms']
        )

    return "Successfully queried Fleet Focus delays main table"

def get_jobs():
    """ Extract work order data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="oracle_fleet")
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
        date_format=conf['date_format_ymd_hms']
        )

    return "Successfully queried Fleet Focus jobs main table"

def get_vehicles():
    """ Extract vehicles data from Fleet Focus """

    credentials = BaseHook.get_connection(conn_id="oracle_fleet")
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
        date_format=conf['date_format_ymd_hms']
        )

    return "Successfully queried Fleet Focus eq_main table"