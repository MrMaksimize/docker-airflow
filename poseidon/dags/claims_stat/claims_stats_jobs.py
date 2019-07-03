import os
import logging
import pandas as pd
import cx_Oracle
import numpy as np
import string as sd

from trident.util import general
from trident.util import geospatial

conf = general.config
credentials = general.source['risk']


prod = conf['prod_data_dir'] # + '/claimstat_datasd.csv'
tmp = conf['temp_data_dir'] #+ 'claimstat_raw.csv'
geocoded_addresses = 'https://datasd-reference.s3.amazonaws.com/claims_address_book.csv'



def get_claims_data():
    """Query an oracle database"""
    logging.info('Retrieving data from Oracle database')
    print(credentials)
    # This requires that otherwise optional credentials variable
    db = cx_Oracle.connect(credentials)
    # Create a sql file containing query for the database
    # Save this file in a sql folder at the same level as the jobs file
    sql= general.file_to_string('./sql/claimstat_tsw.sql', __file__)
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query returned {df.shape[0]} results')

    general.pos_write_csv(
        df,
        "{}/claimstat_raw.csv".format(tmp)) 

    return 'Successfully retrieved Oracle data.'

def clean_geocode_claims():

    # Load the raw data and prepare it for geocoding
    logging.info('Reading query output')
    df = pd.read_csv("{}/claimstat_raw.csv".format(tmp))

    """Keep unique claim numbers only (they will have the same address, only different claimants)
    no need to geocode twice"""
    temp_df=df.drop_duplicates(subset=['CLAIM_NUMBER'], keep='first', inplace=False)

    
    #Load and merge address book 
    logging.info("Reading in address book")
    add_book = pd.read_csv(geocoded_addresses,low_memory=False)
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
    fresh_geocodes = to_geocode.assign(lat=coords[0],lng=coords[1])

   """Before appending to address book, merge back to new_geocodes to include 
   every unique claim number and assign addresses"""
   new_geocodes_m= new_geocodes.loc[:,['CLAIM_NUMBER','ADRESS1','ZIP_CODE']]

   ##Convert zip code to string
   new_geocodes_m.ZIP_CODE= new_geocodes_m.ZIP_CODE.astype(str)

   ##Remove leading and trailing spaces from address and zip code
   new_geocodes_m.ADRESS1=new_geocodes_m.ADRESS1.str.strip()
   new_geocodes_m.ZIP_CODE=new_geocodes_m.ZIP_CODE.str.strip()


   ##merge fresh geocodes (data containing lat longs) and new geocodes_m (data that needed lat longs)

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

###Append to previous address book
new_add_book = pd.concat([add_book,new_geocoded_claims],ignore_index=True)
new_add_book= new_add_book.loc[:,['CLAIM_NUMBER','ADDRESS_geoc', 'lat', 'lng']]


logging.info('Writing final dataset')

new_claims = pd.merge(df,
            new_add_book[['CLAIM_NUMBER','lat','lng']],
            left_on='CLAIM_NUMBER',
            right_on='CLAIM_NUMBER',
            how='left',
            )

updated_df = new_claims.sort_values(by='INCIDENT_DATE', ascending=False)

##add column: exact vs inexact address
split_add = updated_df["ADRESS1"].str.split(" ", n = 1, expand = True) 
split_add['exact']=pd.to_numeric(split_add[0], errors='coerce').notnull()
updated_df['Exact_Address']=split_add['exact']




    logging.info('Writing final dataset')

    general.pos_write_csv(
        updated_df,
        prod_file)

    """0r is it:


    # Write raw data
    general.pos_write_csv(
        df,
        "{}/claimstat_clean.csv".format(prod))"""

    return "Successfully generated claims stat prod file"
