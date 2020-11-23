import cx_Oracle
import pandas as pd
import string
import logging

from trident.util import general

conf = general.config


def get_oracle_data(mode='drinking',**context):
    """ Extract data from Oracle database """

    credentials = conf['oracle_wpl']
    db = cx_Oracle.connect(credentials)
    sql = general.file_to_string(f'./sql/analytes_{mode}.sql', __file__)
    
    df = pd.read_sql_query(sql, db)

    general.pos_write_csv(
    df,
    f"{conf['temp_data_dir']}/analytes_{mode}.csv",
    date_format=conf['date_format_ymd'])

    return f"Successfully queried Oracle data source for {mode}"

def process_data(mode='drinking',**context):
    """ Create production output """

    logging.info(f"Reading in sql output for {mode}")
    df = pd.read_csv(f"{conf['temp_data_dir']}/analytes_{mode}.csv",
        low_memory=False)

    logging.info("Isolate qualifiers and units")
    quals_units = df[['SAMPLE_ID','ANALYTE','QUALIFIER','UNITS']]
    quals_units = quals_units.drop_duplicates()

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
    "FLUORIDE":'Flouride',
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
        
        final = rus_nitrate.copy()
        file_name = 'analyte_tests_drinking_water_datasd'

    elif mode == 'plants':

        final = rus_nitrate.loc[rus_nitrate['analyte'].isin(analytes_effluent)]
        logging.info(f"Filtering on analytes results in {final.shape[0]} records")
        final['analyte'] = final['analyte'].apply(lambda x: analytes_effluent_map.get(x, x))
        file_name = 'analyte_tests_effluent_datasd'

    else:

        raise Exception('Invalid mode')


    general.pos_write_csv(
    final[prod_order],
    f"{conf['prod_data_dir']}/{file_name}.csv",
    date_format=conf['date_format_ymd'])

    return f"Successfully processed prod file for {mode}"

