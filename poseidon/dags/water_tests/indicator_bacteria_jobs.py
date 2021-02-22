import cx_Oracle
import pandas as pd
import os
import string
import logging
import re
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta

from trident.util import general

conf = general.config



def get_indicator_bacteria_tests(date_start='01-JAN-2014', date_end='15-JUN-2017', **kwargs):
    
    # For test mode
    if kwargs['test_mode'] == True:
        logging.warning("RUNNING IN TEST MODE, PULLING LAST YEAR ONLY!!!!")
        date_start = (kwargs['execution_date'] - timedelta(days=365)).strftime('%d-%b-%Y')
    
    #db = cx_Oracle.connect(conf['oracle_wpl'])
    #db = OracleHook(conn_id='WPL')

    credentials = BaseHook.get_connection(conn_id="WPL")
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

    logging.info("Starting Indicator Bac Tests: " + date_start + " to " + date_end)


    jzn_1_q = string.Template(general.file_to_string('./sql/jzn1.sql', __file__))\
        .substitute(ds=date_start, de=date_end)
    jzn_2_q = string.Template(general.file_to_string('./sql/jzn2.sql', __file__))\
        .substitute(ds=date_start, de=date_end)
    jzn_3_q = string.Template(general.file_to_string('./sql/jzn3.sql', __file__))\
        .substitute(ds=date_start, de=date_end)
    jzn_4_q = string.Template(general.file_to_string('./sql/jzn4.sql', __file__))\
        .substitute(ds=date_start, de=date_end)

    logging.info("Reading JZN1")
    jzn_1 = pd.read_sql_query(jzn_1_q, db, coerce_float=True, index_col='F_FIELD_RECORD')
    jzn_1.F_VALUE = pd.to_numeric(jzn_1.F_VALUE, errors='coerce')
    jzn_1 = jzn_1[jzn_1.F_VALUE.notnull()]

    logging.info("Reading JZN2")
    jzn_2 = pd.read_sql_query(jzn_2_q, db, coerce_float=True, index_col='F_FIELD_RECORD')
    jzn_2.F_VALUE = pd.to_numeric(jzn_2.F_VALUE, errors='coerce')
    jzn_2 = jzn_2[jzn_2.F_VALUE.notnull()]

    logging.info("Reading JZN3")
    jzn_3 = pd.read_sql_query(jzn_3_q, db, coerce_float=True, index_col='F_FIELD_RECORD')
    jzn_3.F_VALUE = pd.to_numeric(jzn_3.F_VALUE, errors='coerce')
    jzn_3 = jzn_3[jzn_3.F_VALUE.notnull()]

    logging.info("Reading JZN4")
    jzn_4 = pd.read_sql_query(jzn_4_q, db, coerce_float=True, index_col='F_FIELD_RECORD')
    jzn_4.F_VALUE = pd.to_numeric(jzn_4.F_VALUE, errors='coerce')
    jzn_4 = jzn_4[jzn_4.F_VALUE.notnull()]

    jn_1 = jzn_1.rename(columns={
        'SOURCE':'V5_SOURCE',
        'SAMPLE_DATE':'V5_SAMPLE_DATE',
        'SAMPLE_ID':'V5_SAMPLE_ID',
        'F_VALUE':'V5_CL2_TOTAL',
        'L_VALUE':'V5_T_COLIFORM'
    }).filter(like='V5',axis=1)

    jn_2 = jzn_2.rename(columns={
        'L_VALUE':'V5_E_COLI'
    }).filter(like='V5',axis=1)

    jn_3 = jzn_3.rename(columns={
        'F_QUAL':'V5_TEMP_PART1',
        'F_VALUE':'V5_TEMP_PART2'
    }).filter(like='V5',axis=1)

    jn_4 = jzn_4.rename(columns={
        'F_QUAL':'V5_PH_PART1',
        'F_VALUE':'V5_PH_PART2'
    }).filter(like='V5',axis=1)

    df = jn_1.join([jn_2, jn_3, jn_4], how='inner')

    df = df.rename(columns={
        'V5_PH_PART2':'V5_PH',
        'V5_TEMP_PART2':'V5_TEMPERATURE',
    })

    del df['V5_PH_PART1']
    del df['V5_TEMP_PART1']

    df.columns = [re.sub('V5\_','',x) for x in df.columns]
    df.columns = [x.lower() for x in df.columns]
    df = df.rename(columns={'sample_date':'date_sampled'})
    df.index.rename(name='FR_NUM', inplace=True)

    new_file_path = conf['prod_data_dir'] + '/indicator_bacteria_tests_datasd_v1.csv'
    logging.info("Writing to " + new_file_path)
    df.to_csv(new_file_path,
        index=True, 
        encoding='utf-8', 
        doublequote=True, 
        date_format="%Y-%m-%d")
   
    return "Indicator bacteria tests written to " + new_file_path


def get_latest_bac_tests():
    full_bacs_path = conf['prod_data_dir'] + "/indicator_bacteria_tests_datasd_v1.csv"
    bac_tests = pd.read_csv(full_bacs_path)
    bac_tests.date_sampled = pd.to_datetime(bac_tests.date_sampled, infer_datetime_format=True)

    df = bac_tests[bac_tests.date_sampled == max(bac_tests.date_sampled)]

    new_file_path = conf['prod_data_dir'] + '/latest_indicator_bac_tests_datasd_v1.csv'

    df.to_csv(new_file_path,
        index=False, 
        encoding='utf-8', 
        doublequote=True, 
        date_format="%Y-%m-%d")

    return "Latest indicator bacteria tests written to " + new_file_path
