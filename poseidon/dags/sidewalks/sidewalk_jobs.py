"""_jobs file for sidewalk oci."""
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
from airflow.hooks.mssql_hook import MsSqlHook
import pymssql
from trident.util import general


conf = general.config

cond_file = f"{conf['prod_data_dir']}/sidewalk_cond_datasd_v1.csv"


def get_sidewalk_data(**kwargs):
    """Get sidewalk condition data from DB."""
    sw_query = general.file_to_string('./sql/sidewalk_insp.sql', __file__)
    sw_conn = MsSqlHook(mssql_conn_id='streets_cg_sql')

    df = sw_conn.get_pandas_df(sw_query)

    # Rename columns we're keeping
    df = df.rename(columns={
        'sap_id': 'seg_id',
        'legacy_id': 'geojoin_id',
        'inspectiondate': 'oci_date',
        'rating': 'oci_desc',
        'condition': 'oci'
        })

    df = df.drop(['cgLastModified',
        'MaxInspect',
        'MaxMod'],axis=1)

    # Write csv
    logging.info('Writing ' + str(df.shape[0]))
    
    general.pos_write_csv(
       df, cond_file, date_format="%Y-%m-%d")
    
    return "Successfully wrote prod file"
