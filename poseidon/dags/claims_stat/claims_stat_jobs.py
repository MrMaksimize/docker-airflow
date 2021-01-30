import os
import logging
import pandas as pd
import cx_Oracle
import numpy as np
import string as sd

from trident.util import general
from trident.util import geospatial
from airflow.models import Variable
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook

conf = general.config
prod = conf['prod_data_dir']
tmp = conf['temp_data_dir']
geocoded_addresses = 'claims_address_book.csv'

def get_claims_data():
    """Query an oracle database"""
    logging.info('Retrieving data from Oracle database')
    # This requires that otherwise optional credentials variable
    
    credentials = BaseHook.get_connection(conn_id="RISK")
    
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

    # OracleHook will not work
    #db = OracleHook.get_connection("RISK")

    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    sql= general.file_to_string('./sql/claimstat.sql', __file__)
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        f"{tmp}/claimstat_raw.csv")

    return 'Successfully retrieved Oracle data.'

def clean_geocode_claims():
    """ Clean and geocode data """

    # Load the raw data and prepare it for geocoding
    logging.info('Reading query output')
    df = pd.read_csv(f"{tmp}/claimstat_raw.csv")

    #Keep unique claim numbers only 
    #(they will have the same address, only different claimants)
    #no need to geocode twice
    temp_df = df.drop_duplicates(subset=['CLAIM_NUMBER'], keep='first', inplace=False)


    #Load and merge address book
    logging.info("Reading in address book")
    
    bucket_name=Variable.get('S3_REF_BUCKET')
    s3_url = f"s3://{bucket_name}/reference/{geocoded_addresses}"
    add_book = pd.read_csv(s3_url,low_memory=False)
    
    add_book_join = pd.merge(temp_df,
        add_book,
        how='left',
        left_on='CLAIM_NUMBER',
        right_on='CLAIM_NUMBER',
        indicator=True
    )

    logging.info("Isolating new addresses not in address book")
    new_geocodes = add_book_join.loc[add_book_join['_merge'] == 'left_only']
    old_geocodes = add_book_join.loc[add_book_join['_merge'] == 'both']

    if new_geocodes.empty:
        logging.info("There is nothing new to geocode")
        new_claims=  pd.merge(df,
            add_book[['CLAIM_NUMBER','lat','lng']],
            left_on='CLAIM_NUMBER',
            right_on='CLAIM_NUMBER',
            how='left',
            )

    else:

        to_geocode= new_geocodes.loc[:,['CLAIM_NUMBER','ADRESS1','ZIP_CODE']]

        ## Need to convert zip code to string before applying function
        to_geocode.ZIP_CODE= to_geocode.ZIP_CODE.astype(str)

        ##Remove leading and trailing spaces from address and zip code
        to_geocode.ADRESS1=to_geocode.ADRESS1.str.strip()
        to_geocode.ZIP_CODE=to_geocode.ZIP_CODE.str.strip()

        ### Drop duplicates in terms of address-zip code
        to_geocode= to_geocode.drop_duplicates(subset=['ADRESS1','ZIP_CODE'])

        ##Drop rows with missing address
        to_geocode=to_geocode.dropna(subset=['ADRESS1'])


        ##how many addresses do we need to geocodde
        logging.info(f"Need to geocode {to_geocode.shape[0]}")

        #Apply geocoding function
        geocoder_results=to_geocode.apply(lambda x: geospatial.census_address_geocoder(address_line=x['ADRESS1'], zip= x['ZIP_CODE'], bounds='yes'),axis=1)

        coords = geocoder_results.apply(pd.Series)
        logging.info(coords.head())
        fresh_geocodes = to_geocode.assign(lat=coords[0],lng=coords[1])

        # Before appending to address book, merge back to new_geocodes to include
        # every unique claim number and assign addresses
        new_geocodes_m= new_geocodes.loc[:,['CLAIM_NUMBER','ADRESS1','ZIP_CODE']]

        ##Convert zip code to string
        new_geocodes_m.ZIP_CODE= new_geocodes_m.ZIP_CODE.astype(str)

        ##Remove leading and trailing spaces from address and zip code
        new_geocodes_m.ADRESS1=new_geocodes_m.ADRESS1.str.strip()
        new_geocodes_m.ZIP_CODE=new_geocodes_m.ZIP_CODE.str.strip()


        ##merge fresh geocodes (data containing lat longs) and new geocodes_m (data that needed lat longs)
        logging.info('Merging geocodes to get claim number')
        new_geocoded_claims = pd.merge(new_geocodes_m,
        fresh_geocodes,
        how='left',
        left_on=['ADRESS1','ZIP_CODE'],
        right_on=['ADRESS1', 'ZIP_CODE'],
        indicator= True
        )

        logging.info("Adding new geocodes to address book and data")

        """We are ready to append newly geocoded claims to address book, first we need to
        change name of CLAIM_NUMBER_x to CLAIM_NUMBER, from ADRESS1 to ADDRESS_geoc and
        keep only relevant columns"""

        new_geocoded_claims.rename(columns={'CLAIM_NUMBER_x': 'CLAIM_NUMBER'}, inplace=True)
        new_geocoded_claims.rename(columns={'ADRESS1': 'ADDRESS_geoc'}, inplace=True)

        new_geocoded_claims=new_geocoded_claims.loc[:,['CLAIM_NUMBER','ADDRESS_geoc', 'lat', 'lng']]
        logging.info('Appending new geocodes to address book')
        ### Append to previous address book
        new_add_book = pd.concat([add_book,new_geocoded_claims],ignore_index=True)
        new_add_book= new_add_book.loc[:,['CLAIM_NUMBER','ADDRESS_geoc', 'lat', 'lng']]
        logging.info('Writing address book')
        # Write new address book to temp directory
        general.pos_write_csv(new_add_book,
        f"{prod}/claims_address_book.csv")

        logging.info('merging lat lngs to final data')

        new_claims = pd.merge(df,
                new_add_book[['CLAIM_NUMBER','lat','lng']],
                left_on='CLAIM_NUMBER',
                right_on='CLAIM_NUMBER',
                how='left',
                )

    
    logging.info('Sorting by incident date')
    updated_df = new_claims.sort_values(by='INCIDENT_DATE', ascending=False)

    ##add column: exact vs inexact address
    split_add = updated_df["ADRESS1"].str.split(" ", n = 1, expand = True)
    split_add['exact']=pd.to_numeric(split_add[0], errors='coerce').notnull()
    updated_df['Exact_Address']=split_add['exact']

    logging.info('Writing final dataset')

    # Write clean data
    general.pos_write_csv(updated_df,
        f"{prod}/claim_stat_datasd.csv")

    return "Successfully generated claims stat prod file"

def claims_by_department(org_name='',
    claim_orgs=[],
    **kwargs):
    
    df_temp = pd.read_csv(f"{prod}/claim_stat_datasd.csv")
    df_dept = df_temp.loc[df_temp["ORGANIZATION_DESC"].isin(claim_orgs),:]
    general.pos_write_csv(df_dept,
        f'{prod}/claims_clean_datasd_{org_name}.csv')

    return f"Successfully write {org_name} csv!"

def deploy_dashboard(org_name):
    """Deploy Claims Stat dashboard"""

    command = "Rscript /usr/local/airflow/poseidon/trident/util/shiny_deploy.R " \
    + f"--appname=claims_{org_name}_{conf['env'].lower()} " \
    + f"--path=/usr/local/airflow/poseidon/dags/claims_stat/claims_{org_name}.Rmd " \
    + f"--name={Variable.get('SHINY_ACCT_NAME')} " \
    + f"--token={Variable.get('SHINY_TOKEN')} " \
    + f"--secret={Variable.get('SHINY_SECRET')} " \
    + "--force=TRUE "

    return command
