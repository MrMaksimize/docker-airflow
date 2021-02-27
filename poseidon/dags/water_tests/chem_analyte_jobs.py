import cx_Oracle
import pandas as pd
import string
import logging
from datetime import datetime as dt
from airflow.hooks.base_hook import BaseHook

from trident.util import general

conf = general.config

def get_oracle_data(mode='drinking',**context):
    """ Extract data from Oracle database """

    exec_date = context['next_execution_date'].in_tz(tz='US/Pacific')
    today = exec_date.subtract(days=1)

    this_yr = today.year
    this_mo = today.strftime("%m")
    this_day = today.strftime("%d")

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

    sql = general.file_to_string(f'./sql/analytes_{mode}.sql', __file__)
    
    sql += f"AND (TRUNC(SAMPLE.SAMPLE_DATE) >= DATE '{this_yr}-01-01') " \
    + f"AND (TRUNC(SAMPLE.SAMPLE_DATE) < DATE '{this_yr}-{this_mo}-{this_day}' )) " \
    + "ORDER BY SAMPLE.SAMPLE_DATE, SAMPLE.SOURCE, " \
    + "RESULT.ANALYTE, RESULT.TEST_NUMBER"
    

    df = pd.read_sql_query(sql, db)

    fname_yr = f"{this_yr}"

    general.pos_write_csv(
    df,
    f"{conf['temp_data_dir']}/analytes_{mode}_{fname_yr}.csv",
    date_format="%Y-%m-%d")

    return fname_yr

def process_data(mode='drinking',**context):
    """ Create production output """

    fname_yr = context['task_instance'].xcom_pull(dag_id=f"chem_analytes.get_create_{mode}",
        task_ids=f'get_{mode}_sql')


    logging.info(f"Reading in sql output for {mode}")
    df = pd.read_csv(f"{conf['temp_data_dir']}/analytes_{mode}_{fname_yr}.csv",
        low_memory=False)

    logging.info("Isolate qualifiers and units")
    quals_units = df[['SAMPLE_ID','ANALYTE','QUALIFIER','UNITS']]
    # Inconsistencies in having qualifier filled out cause us to have to
    # sort before deduplicating
    # This sort and dedupe will keep any rows that have a qualifier
    # for that analyte. It does not control for situations
    # Where an analyte has different qualifiers
    quals_units = quals_units.sort_values(['SAMPLE_ID','ANALYTE','QUALIFIER','UNITS'],na_position='last')
    quals_units = quals_units.drop_duplicates(['SAMPLE_ID','ANALYTE'])

    logging.info("Isolate sources and descriptions")
    source_desc = df[['SOURCE','DESCRIPTION']]
    source_desc = source_desc.drop_duplicates()
    
    logging.info("Get mean results for multiple tests")
    mean_results = df.groupby(['SAMPLE_DATE',
        'SOURCE',
        'SAMPLE_ID',
        'ANALYTE']).agg({'VALUE':'mean'}).reset_index()

    logging.info(f"Resulted in {mean_results.shape[0]} total results")

    exp_results = df.groupby(['SAMPLE_ID','ANALYTE']).ngroups
    logging.info(f"This compares to {exp_results} expected results")

    mean_results['VALUE'] = mean_results['VALUE'].apply(lambda x: round(x,3))
    logging.info(f"Mean results has {mean_results.shape[0]} records")

    logging.info("Merge units back")
    results_units = pd.merge(mean_results,
        quals_units,
        how='left',
        on=['SAMPLE_ID','ANALYTE'])

    logging.info("Merge sources back")
    results_units_sources = pd.merge(results_units,
        source_desc,
        how='left',
        on='SOURCE')

    logging.info(f"Final merge has {results_units_sources.shape[0]} records")

    results_units_sources.columns = ['date_sample',
    'sample_source',
    'sample_id',
    'analyte',
    'analyte_value',
    'value_qualifier',
    'value_units',
    'source_description']

    logging.info("Creating Nitrate as N")

    nitrate = results_units_sources.loc[results_units_sources['analyte'] == "NITRATE"].copy()
    
    if nitrate.empty:
        logging.info("No Nitrate results to convert")
        rus_nitrate = results_units_sources.copy()

    else:
        logging.info(f"Have {nitrate.shape[0]} records for Nitrate")
        nitrate.loc[:,'analyte'] = "Nitrate (as N)"
        nitrate.loc[:,'analyte_value'] = nitrate.loc[:,'analyte_value'].apply(lambda x: round(x*0.2259,4))
        rus_nitrate = pd.concat([results_units_sources,nitrate],ignore_index=True)
    
    logging.info(f"Ending with {rus_nitrate.shape[0]} records after adding Nitrate as N")

    prod_order = ['date_sample',
    'sample_source',
    'sample_id',
    'analyte',
    'value_qualifier',
    'analyte_value',
    'value_units',
    'source_description']

    analytes_effluent_map = {"PH":'pH',
    "CONDUCTIVITY":'Conductivity',
    "ALKALINITY_TOT":'Total Alkalinity',
    "TDS":'Total Dissolved Solids',
    "HARDNESS_TOTAL":'Total Hardness',
    "HARDNESS_CA":'Calcium Hardness',
    "CARBONATE":'Carbonate',
    "BICARBONATE":'Bicarbonate',
    "TURBIDITY":'Turbidity',
    "COLOR":'Color',
    "BROMIDE":'Bromide',
    "CHLORIDE":'Chloride',
    "FLUORIDE":'Fluoride',
    "NITRATE":'Nitrate',
    "Nitrate (as N)":'Nitrate (as N)',
    "PHOSPHATE_O":'Ortho Phosphate',
    "NITROGEN_TOTAL":'Total Nitrogen',
    "SILICA":'Silica',
    "SULFATE":'Sulfate',
    "ALUMINUM":'Aluminum',
    "CALCIUM":'Calcium',
    "COPPER":'Copper',
    "IRON":'Iron',
    "LEAD":'Lead',
    "MANGANESE":'Manganese',
    "MAGNESIUM":'Magnesium',
    "POTASSIUM":'Potassium',
    "SODIUM":'Sodium',
    "ZINC":'Zinc',
    "AGGRESSIVE":'Aggressive Index',
    "LANGLIER":'Langlier Index',
    "TOC":'Total Organic Carbon',
    "TOTAL_THM":'Total THMs',
    "HAA5":'HAA5',
    "BROMATE":'Bromate',
    "CHLORITE":'Chlorite',
    "CHLORATE":'Chlorate',
    "PERCHLORATE":'Perchlorate'}

    analytes_effluent = [*analytes_effluent_map]

    if mode == 'drinking':
        
        current = rus_nitrate.copy()
        file_name = f'analyte_tests_drinking_water_datasd'
        
        logging.info("Reading in prod file")
        prod_df = pd.read_csv(f"{conf['prod_data_dir']}/{file_name}.csv",
            low_memory=False)

    elif mode == 'plants':

        current = rus_nitrate.loc[rus_nitrate['analyte'].isin(analytes_effluent)].copy()
        logging.info(f"Filtering on analytes results in {current.shape[0]} records")
        current['analyte'] = current['analyte'].apply(lambda x: analytes_effluent_map.get(x, x))
        
        file_name = f'analyte_tests_effluent_datasd'

        logging.info("Reading in prod file")
        prod_df = pd.read_csv(f"{conf['prod_data_dir']}/{file_name}.csv",
            low_memory=False)

    else:

        raise Exception('Invalid mode')

    logging.info(f"Prod file has {prod_df.shape[0]} records")

    final = pd.concat([current[prod_order],prod_df],ignore_index=True)

    logging.info(f"Concat has {final.shape[0]} records")

    final = final.drop_duplicates(subset=['date_sample',
        'sample_source',
        'sample_id',
        'analyte'])

    logging.info(f"Final has {final.shape[0]} records after deduplicating")


    general.pos_write_csv(
    final,
    f"{conf['prod_data_dir']}/{file_name}.csv",
    date_format="%Y-%m-%d")

    return f"Successfully processed prod file for {mode}"

