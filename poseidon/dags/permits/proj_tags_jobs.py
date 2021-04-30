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
from airflow.hooks.base_hook import BaseHook
import cx_Oracle

conf = general.config

def get_tags_file(**context):
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

    sql= general.file_to_string(f'./sql/01_dsd_projecttags.sql', __file__)
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
    df,
    f"{conf['temp_data_dir']}/01_dsd_projecttags.csv")

    return 'Successfully retrieved Oracle data.' 

def build_tags(**context):
    """Get PTS permits and create active and closed"""

    dtypes = {'DEVEL_NUM':'str',
    'PROJ_ID':'str',
    'PROJ_TAG_ID':'str'}


    logging.info("Reading in project tag file")
    df = pd.read_csv(f"{conf['temp_data_dir']}/01_dsd_projecttags.csv",
        low_memory=False,
        dtype=dtypes)

    logging.info("File read successfully, renaming columns")

    df.columns = [x.lower() for x in df.columns]

    df = df.rename(columns={'devel_num':'development_id',
        'proj_id':'project_id',
        'proj_scope':'project_scope',
        'proj_tag_id':'project_tag_id',
        'description':'project_tag_desc'
        })

    logging.info("Writing prod file")
    general.pos_write_csv(
    df,
    f"{conf['prod_data_dir']}/permits_set1_project_tags_datasd.csv")

    logging.info(f"Writing compressed csv")
    general.sf_write_csv(df,
        'dsd_proj_tags')

    return 'Created new project tags file'