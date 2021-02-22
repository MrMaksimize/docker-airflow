"""PD ripa _jobs file."""
import pandas as pd
import numpy as np
import glob
import os
import re
import csv
from subprocess import Popen, PIPE
import subprocess
from shlex import quote
import logging
from trident.util import general
from airflow.hooks.base_hook import BaseHook

conf = general.config


def get_data():
    """Download RIPA data from FTP."""

    ftp_conn = BaseHook.get_connection(conn_id="FTP_DATASD")

    temp_dir = conf['temp_data_dir']

    # Sticking to wget for this 
    # because file names change drastically
    command = "wget -np --continue " \
        + f"--user={ftp_conn.login} " \
        + f"--password='{ftp_conn.password}' " \
        + f"--directory-prefix={temp_dir} " \
        + "ftp://ftp.datasd.org/uploads/sdpd/" \
        + "ripa/*.xlsx"

    command = command.format(quote(command)) 

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
        
    if p.returncode != 0:
        logging.info("Error downloading files")
        raise Exception(p.returncode)
    else:
        logging.info("Files downloaded")
        filename = f"{conf['temp_data_dir']}/*RIPA*.xlsx"
        list_of_files = glob.glob(filename)
        logging.info(list_of_files)
        latest_file = max(list_of_files, key=os.path.getmtime)
        return latest_file

def process_excel(**context):
    """Process RIPA data."""
    latest_file = context['task_instance'].xcom_pull(dag_id="pd_ripa",
        task_ids='get_data')
    
    logging.info(f"Reading in {latest_file}")

    ripa = pd.read_excel(latest_file,
        engine='openpyxl',
        sheet_name=None)

    keys = [*ripa]

    for key in keys:
        logging.info(f"Starting with {key}")
        # Names need underscores where each capital letter is
        filename = re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1_', key)
        df = ripa[key]
        logging.info(f"df has {df.shape[0]} rows")
        df.columns = df.columns.str.replace(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1_').str.lower()
        df = df.rename(columns={'id':'stop_id',
                                              'stopdate':'date_stop',
                                              'stoptime':'time_stop',
                                              'block':'address_block',
                                              'street':'address_street',
                                              'cityname':'address_city'
                                             })
        float_cols = df.select_dtypes(include=['float'])
        if float_cols.empty:
            print('No float columns')
        else:
            float_col_names = float_cols.columns.values
            df.loc[:,float_col_names] = df.loc[:,float_col_names].fillna(-999999.0).astype(int)
            df = df.replace(-999999,'')
            df.loc[:,float_col_names] = df.loc[:,float_col_names].astype(str)
            
        outfile = f"{conf['temp_data_dir']}/ripa_{filename.lower()}.csv"
        logging.info(f"Writing {key} to csv")
        general.pos_write_csv(
            df,
            outfile)

    return 'Successfully processed new ripa files'

def process_prod_files(mode='stops',**context):
    """ 
    Append new data to each prod file 
    """

    outfile = f"{conf['prod_data_dir']}/ripa_{mode}_datasd.csv"

    logging.info(f"Reading in new {mode} file")
    new_df = pd.read_csv(
        f"{conf['temp_data_dir']}/ripa_{mode}.csv",
        low_memory=False
        )
    cols = new_df.columns.to_list()
    logging.info(f"Reading in prod {mode} file")

    try:
        prod_df = pd.read_csv(outfile,low_memory=False)
        logging.info(f'Prod file found with {prod_df.shape[0]} rows')
    
    except Exception as e:
        prod_df = pd.DataFrame(columns=cols)        
        logging.info(f'No prod file found for {mode}. Creating from new data.....')
    

    logging.info("Combining them")
    df = pd.concat([prod_df,new_df])
    logging.info(f"Concat has {df.shape} cols & rows")
    df = df.drop_duplicates()
    logging.info(f"After dedupe, result has {df.shape} cols & rows")
    logging.info("Sorting and writing data")
    df = df.sort_values(['stop_id','pid'])

    general.pos_write_csv(
        df,
        outfile)

    return f"Successfully created {mode} ripa prod file"
