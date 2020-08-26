""" Fleet _jobs file """

# Required imports

from trident.util import general
import logging

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

    credentials = general.source['fleet']
    db = cx_Oracle.connect(credentials)
    sql= general.file_to_string('./sql/delays-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{temp_path}/fleet_delays.csv")

    return "Successfully queried Fleet Focus delays main table"

def get_jobs():
    """ Extract work order data from Fleet Focus """

    credentials = general.source['fleet']
    db = cx_Oracle.connect(credentials)
    sql= general.file_to_string('./sql/jobs-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{temp_path}/fleet_jobs.csv")

    return "Successfully queried Fleet Focus jobs main table"

def get_vehicles():
    """ Extract vehicles data from Fleet Focus """

    credentials = general.source['fleet']
    db = cx_Oracle.connect(credentials)
    sql= general.file_to_string('./sql/vehicles-query.sql', __file__)
    
    # This pulls in query results as df
    df = pd.read_sql_query(sql, db)

    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{temp_path}/fleet_vehicles.csv")

    return "Successfully queried Fleet Focus eq_main table"