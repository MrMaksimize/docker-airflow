"""City docs _jobs file."""
import os
import string
import pandas as pd
import logging
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general
import dags.city_docs.documentum_name as dn

conf = general.config
#prod_data_schedule_24 = general.create_path_if_not_exists(conf['prod_data_dir'] + '/schedule_24')
#prod_data_schedule_others = general.create_path_if_not_exists(conf['prod_data_dir'] + '/schedule_others')

def fix_title(df):
    df = df.fillna(value='')
    df['TITLE'] = df['TITLE'].str.strip()
    object_names = df.loc[(df['TITLE'] == ''),'OBJECT_NAME']
    df.loc[(df['TITLE'] == ''),'TITLE'] = object_names
    df['TITLE'] = df['TITLE'].str.slice(stop=254)
    return df['TITLE']

def get_sire():
    """Get tables from Sire."""
    logging.info('Getting files sire')
    for root, dirs, files in os.walk('./poseidon/poseidon/dags/city_docs/sql/sire'):
        for name in files:
            logging.info('Querying for '+name)
            path = './sql/sire/{}'.format(name)
            query_string = general.file_to_string(path, __file__)
            logging.info('Connecting to MS Database')
            sire_conn = MsSqlHook(mssql_conn_id='sire_sql')
            logging.info('Reading data to Pandas DataFrame')
            df = sire_conn.get_pandas_df(query_string)
            table_type = name[0:-4]

            logging.info('Correcting title column')
            df['TITLE'] = fix_title(df[['TITLE','OBJECT_NAME']])

            logging.info('Write Production file')
            save_path = '{}/sire_{}.csv'.format(conf['prod_data_dir'],table_type)
            general.pos_write_csv(df, save_path)

    return "Successfully retrieved Sire tables"

def get_onbase():
    """Get tables from OnBase."""
    logging.info('Getting files from onbase')
    for root, dirs, files in os.walk('./poseidon/poseidon/dags/city_docs/sql/onbase'):
        for name in files:
            logging.info('Querying for '+name)
            path = './sql/onbase/{}'.format(name)
            query_string = general.file_to_string(path, __file__)
            logging.info('Connecting to MS Database')
            onbase_conn = MsSqlHook(mssql_conn_id='onbase_sql')
            logging.info('Reading data to Pandas DataFrame')
            df = onbase_conn.get_pandas_df(query_string)
            table_type = name[0:-4]

            logging.info('Correcting title column')
            df['TITLE'] = fix_title(df[['TITLE','OBJECT_NAME']])

            save_path =  '{0}/onbase_{1}.csv'.format(conf['prod_data_dir'],table_type)
            logging.info('Writting Production file')
            general.pos_write_csv(df, save_path)

    return "Successfully retrieved OnBase tables"
    
def get_documentum(mode, **kwargs):
    """Get tables from Documentum."""
    logging.info('Getting files from documentum')
    table_name = dn.table_name(mode)
    for name in table_name:
        logging.info('Querying for {0} table'.format(name))
        query_string = 'SELECT * FROM SCSLEGIS.dbo.{0};'.format(name)
        logging.info('Connecting to MS Database')
        documentum_conn = MsSqlHook(mssql_conn_id='docm_sql')
        logging.info('Reading data to Pandas DataFrame')
        df = documentum_conn.get_pandas_df(query_string)

        logging.info('Correcting title column')
        
        df['TITLE'] = fix_title(df[['TITLE','OBJECT_NAME']])

        if mode == 'schedule_24':
            save_path =  conf['prod_data_dir'] + '/documentum_{0}.csv'.format(name.lower())
        else:
            save_path =  conf['prod_data_dir'] + '/documentum_{0}.csv'.format(name.lower())
        logging.info('Writing Production file')
        general.pos_write_csv(df, save_path)

    return "Successfully retrieved Documentum tables"