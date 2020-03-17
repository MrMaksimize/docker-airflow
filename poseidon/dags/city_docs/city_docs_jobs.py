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
    for root, dirs, files in os.walk('./poseidon/dags/city_docs/sql/sire'):
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
    for root, dirs, files in os.walk('./poseidon/dags/city_docs/sql/onbase'):
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

def get_onbase_test():
    """Get tables from OnBase."""
    logging.info('Getting files from onbase')
    for root, dirs, files in os.walk('./poseidon/dags/city_docs/sql/onbase'):
        for name in files:
            logging.info('Querying for '+name)
            path = './sql/onbase/{}'.format(name)
            query_string = general.file_to_string(path, __file__)
            logging.info('Connecting to MS Database')
            onbase_conn = MsSqlHook(mssql_conn_id='onbase_test_sql')
            logging.info('Reading data to Pandas DataFrame')
            df = onbase_conn.get_pandas_df(query_string)
            table_type = name[0:-4]

            logging.info('Correcting title column')
            df['TITLE'] = fix_title(df[['TITLE','OBJECT_NAME']])

            save_path =  '{0}/onbase_test_{1}.csv'.format(conf['prod_data_dir'],table_type)
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

        save_path =  conf['prod_data_dir'] + '/documentum_{0}.csv'.format(name.lower())
        logging.info('Writing Production file')
        general.pos_write_csv(df, save_path)

    return "Successfully retrieved Documentum tables"

def get_documentum_test():
    """Get tables from Documentum test database."""
    logging.info('Getting files for documentum test')
    table_name = dn.table_name('schedule_daily')+dn.table_name('schedule_hourly_15')+dn.table_name('schedule_hourly_30')
    logging.info(table_name)
    for name in table_name:
        logging.info('Querying for {0} table'.format(name))
        query_string = 'SELECT * FROM SCSLEGIS.dbo.{0};'.format(name)
        logging.info('Connecting to MS Database')
        documentum_conn = MsSqlHook(mssql_conn_id='docm_test_sql')
        logging.info('Reading data to Pandas DataFrame')
        try:
            df = documentum_conn.get_pandas_df(query_string)
            logging.info('Correcting title column')
        
            df['TITLE'] = fix_title(df[['TITLE','OBJECT_NAME']])

            save_path =  conf['prod_data_dir'] + '/documentum_{0}_test.csv'.format(name.lower())
            general.pos_write_csv(df, save_path)

        except Exception as e:
            logging.info(f'Could not read {0} because {e}')

    return "Successfully retrieved Documentum tables"

def split_reso_ords():
    """Split largest table of reso and ords"""
    filename = 'documentum_scs_council_reso_ordinance_v.csv'
    save_path = f"{conf['prod_data_dir']}/documentum_scs_council_reso_ordinance_v"
    df = pd.read_csv(f"{conf['prod_data_dir']}/{filename}",
        low_memory=False)

    total_records = df.shape[0]
    record_count = 0

    logging.info(f"Dividing {total_records} records")

    df['DOC_DATE'] = pd.to_datetime(df['DOC_DATE'],errors='coerce')

    div_years = [1976,1986,1996,2006,2016]

    for i,year in enumerate(div_years):
        if i == 0:
            sub_div = df.loc[df['DOC_DATE'] < f"01/01/{year}"]
            general.pos_write_csv(sub_div, f"{save_path}_begin_{year-1}.csv")
            logging.info(f"Wrote begin_{year-1}")
            record_count += sub_div.shape[0]
        else:
            sub_div = df.loc[(df['DOC_DATE'] < f"01/01/{year}") & (df['DOC_DATE'] >= f"01/01/{div_years[i-1]}")]
            general.pos_write_csv(sub_div, f"{save_path}_{div_years[i-1]}_{year-1}.csv")
            logging.info(f"Wrote {div_years[i-1]}_{year-1}")
            record_count += sub_div.shape[0]

    df_invalid = df.loc[df['DOC_DATE'].isnull()]
    general.pos_write_csv(df_invalid, f"{save_path}_invalid.csv")
    logging.info("Wrote records with invalid date")
    record_count += df_invalid.shape[0]

    return f"Successfully divided {record_count} from {filename}"

def latest_res_ords(filename):
    """Get last decade from reso and ords table"""

    save_path = f"{conf['prod_data_dir']}/{filename}"
    df = pd.read_csv(f"{conf['prod_data_dir']}/{filename}.csv",
        low_memory=False)

    df['DOC_DATE'] = pd.to_datetime(df['DOC_DATE'],errors='coerce')

    df_current = df.loc[df['DOC_DATE'] >= f"01/01/2016"]
    general.pos_write_csv(df_current, f"{save_path}_2016_current.csv")
    logging.info(f"Wrote 2016_current")

    return f"Successfully extracted this decade of resos and ords"