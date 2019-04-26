import os
import logging
import pandas as pd
import cx_Oracle

from trident.util import general
from trident.util import geospatial

conf = general.config

prod = conf['prod_data_dir']
tmp = conf['temp_data_dir']
credentials = general.source['risk']

def get_claims_data():
    """Query an oracle database"""
    logging.info('Retrieving data from Oracle database')
    print(credentials)
    # This requires that otherwise optional credentials variable
    db = cx_Oracle.connect(credentials)
    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    sql = "SELECT * FROM CLAIMSTAT.CLAIMSTAT"
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        "{}/claimstat_raw.csv".format(tmp))

    return 'Successfully retrieved Oracle data.'

def clean_geocode_claims():
    # Load the raw data
    df = pd.read_csv("{}/claimstat_raw.csv".format(tmp))

    # Do some stuff to it

    # Write raw data
    general.pos_write_csv(
        df,
        "{}/claimstat_clean.csv".format(prod))

