"""This module contains jobs for extracting data out of TTCS."""

import os
import logging
import pandas as pd
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook
import cx_Oracle
import string
import datetime as dt
import numpy as np
from airflow.models import Variable
from functools import reduce
from arcgis import GIS
from urllib.parse import unquote

from trident.util import general
from trident.util import geospatial

conf = general.config

clean_all = conf['temp_data_dir'] + '/ttcs_all_clean.csv'
geocoded_active = conf['temp_data_dir'] + '/ttcs_all_geocoded.csv'
geocoded_addresses = 'ttcs_address_book.csv'

#: Helper function
def prod_files_prep(subset):

    df = subset.drop(['create_yr'],axis=1)
    df = df.sort_values(by=['account_key',
        'date_account_creation'],
        ascending=[True,
        False])
    return df

#: Helper function
def combine_phone(row):
    
    phone_no = ("-").join([row['PHONE_AREA_CD'],row['PHONE_NO']])
    
    if not pd.isna(row['PHONE_EXTENSION']):
        return f"{phone_no}x{row['PHONE_EXTENSION']}"
    else:
        return phone_no

#: DAG function
def query_ttcs(mode='main',**context):
    """Query DB for 'Active Businesses' and save data to temp."""
    logging.info('Retrieving main business tax license data')
    credentials = BaseHook.get_connection(conn_id="TTCS")
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
    sql = general.file_to_string(f'./sql/ttcs-{mode}.sql', __file__)
    df = pd.read_sql_query(sql, db)
    logging.info(f'Query for {mode} returned {df.shape[0]} results')

    if mode == "pins":
        output_file = f"{conf['prod_data_dir']}/ttcs-{mode}.csv"
    else:
        output_file = f"{conf['temp_data_dir']}/ttcs-{mode}.csv"
    
    general.pos_write_csv(
        df,
        output_file,
        date_format="%Y-%m-%d %H:%M:%S")

    return f'Successfully retrieved {mode} from TTCS'

#: DAG function
def clean_data():
    """Clean business license data coming from TTCS."""
    logging.info('Reading various query output')

    df = pd.read_csv(f"{conf['temp_data_dir']}/ttcs-main.csv",
                   low_memory=False,
                   dtype={'ACCOUNT_KEY':str})
    
    tables = ['location','dba','phone','email']

    for table in tables:
        temp_df = pd.read_csv(f"{conf['temp_data_dir']}/ttcs-{table}.csv",
                   low_memory=False,
                   dtype={'ACCOUNT_KEY':str})
        
        if table == 'phone':
            # Do not have a date effective for phone number
            # To minimize duplicates
            temp_df = temp_df.drop_duplicates()
            temp_df['phone_full'] = temp_df.apply(combine_phone,axis=1)
            merge_df = temp_df.groupby(['ACCOUNT_KEY']).agg({'phone_full': ','.join})
        else:
            merge_df = temp_df.copy()

        df = df.merge(merge_df,how='left',on='ACCOUNT_KEY')
        logging.info(f"Merge with {table} resulted in {df.shape}")
   
    df.columns = [x.lower() for x in df.columns]

    logging.info('Creating NAICS sector')

    df['naics_sector'] = df['naics_code'].apply(lambda x: str(x)[:2])

    logging.info(f'Processed {df.shape[0]} businesses')

    df = df.sort_values(by=['account_key',
        'creation_dt'],
        ascending=[True,
        False])

    logging.info('Writing final data to csv')

    general.pos_write_csv(
        df,
        clean_all,
        date_format="%Y-%m-%d")
    return 'Successfully cleaned TTCS data.'

#: DAG function
def geocode_data():
    """Geocode new entries from clean files."""
    logging.info('Geocoding new entries from clean files.')
    
    address_dtype = {'street_no':str,
    'street_pre_direction':str,
    'street_name':str,
    'street_suffix':str,
    'street_fraction':str,
    'city':str,
    'state':str,
    'zip':str
    }
    
    df = pd.read_csv(clean_all,
                     low_memory=False,
                     dtype=address_dtype
                     )

    logging.info(f"Starting with {df.shape} records")

    logging.info('Get address book')
    bucket_name=Variable.get('S3_REF_BUCKET')
    s3_url = f"s3://{bucket_name}/reference/{geocoded_addresses}"
    add_book = pd.read_csv(s3_url,
        low_memory=False,
        dtype=address_dtype
        )

    logging.info('Create uid based on address for merging')

    temp_df = df.fillna(value={'street_no': '',
        'street_pre_direction': '',
        'street_name': '',
        'street_suffix': '',
        'street_fraction': '',
        'city': '',
        'state': '',
        'zip': ''
        })

    add_match = temp_df[['street_no',
    'street_pre_direction' ,
    'street_name',
    'street_suffix',
    'street_fraction',
    'city',
    'state',
    'zip']].apply(lambda x: ''.join(x.map(str)), axis=1)
    temp_df = temp_df.assign(uid=add_match)

    logging.info('Merging address book with temp file')
    add_merge = pd.merge(temp_df,
        add_book[['uid','latitude','longitude']],
        how='left',
        left_on='uid',
        right_on='uid',
        indicator=True
        )

    logging.info('Separating matches from unmatched')

    add_matched = add_merge[add_merge['_merge'] == 'both']
    add_unmatched = add_merge[add_merge['_merge'] == 'left_only']

    if add_unmatched.empty:
        logging.info('Nothing to geocode')

        logging.info('Write final file')

        add_merge = add_merge.drop(['_merge'],axis=1)

        general.pos_write_csv(
                add_merge,
                geocoded_active,
                date_format="%Y-%m-%d")

    else:
        logging.info('Creating subset of unmatched suitable for geocoding')

        add_unmatched = add_unmatched.drop(['latitude','longitude'],axis=1)

        to_geocode = add_unmatched[
            (add_unmatched['po_box'].isnull()) & 
            (add_unmatched['pmb_box'].isnull()) &
            (~add_unmatched['street_no'].str.startswith("0")) &
            (add_unmatched['street_name'] != '') &
            (add_unmatched['street_no'] != '')
            ]

        # to_geocode is a subset of addresses that aren't missing information
        # and aren't PO boxes

        if to_geocode.empty:

            logging.info('Nothing to geocode')

            logging.info('Write final file')

            add_merge = add_merge.drop(['_merge'],axis=1)

            logging.info(f"Ending with {add_merge.shape} records")

            general.pos_write_csv(
                add_merge,
                geocoded_active,
                date_format="%Y-%m-%d")

        else:

            geocode_address = to_geocode[['street_no',
            'street_pre_direction',
            'street_name',
            'street_suffix']].apply(lambda x: ' '.join(x), axis=1)            
            
            zip_split = to_geocode['zip'].str.split('-',expand=True)
            
            to_geocode = to_geocode.assign(address_full=geocode_address,zip_short=zip_split[0])
            
            # merge geocode address back to add_unmatched for later merging
            # This is because address full contains addresses that are similar
            # enough to have the same UID with diff address information
            # Which caused duplicate records later on

            au_w_add_full = pd.merge(add_unmatched,
                to_geocode[['address_full']],
                right_index=True,
                left_index=True,
                how='left'
                )

            general.pos_write_csv(
                au_w_add_full,
                f"{conf['prod_data_dir']}/merge_test.csv")

            logging.info(f"add_umatched had {add_unmatched.shape} records")
            logging.info(f"add_umatched with added address full has {au_w_add_full.shape} records")

            geocode_dedupe = to_geocode.drop_duplicates(subset=['address_full',
                'city',
                'state',
                'zip_short'])
                        
            logging.info(f'Need to geocode {geocode_dedupe.shape[0]}')

            geocoder_results = geocode_dedupe.apply(lambda x: geospatial.census_address_geocoder(address_line=x['address_full'],locality=x['city'],state=x['state'],zip=x['zip_short']), axis=1)

            logging.info('Adding new coords to the df')
            coords = geocoder_results.apply(pd.Series)

            # New coordinates get added to the deduplicated geocode set
            fresh_geocodes = geocode_dedupe.assign(latitude=coords[0],longitude=coords[1])

            logging.info('Merging geocodes to deduped geocode df')
            geocoded = pd.merge(geocode_dedupe,
                fresh_geocodes[['address_full',
                'city',
                'state',
                'zip_short',
                'latitude',
                'longitude']],
                how='left',
                left_on=['address_full',
                'city',
                'state',
                'zip_short'],
                right_on=['address_full',
                'city',
                'state',
                'zip_short'])

            # with this merge, the deduplicated geocoded df
            # is matched with 

            logging.info("Merging geocoded df back to unmatched addresses")
            # This can cause extra records if a previously geoc

            geocoded_unmatched = pd.merge(au_w_add_full,
                geocoded[['uid','latitude','longitude','address_full','zip_short']],
                how='left',
                on=['uid','address_full'])

            logging.info("Concat addresses matched with address book and addresses geocoded")
            geocoded_all = pd.concat([add_matched,geocoded_unmatched],ignore_index=True,sort=True)
            geocoded_all = geocoded_all.drop(['_merge'],axis=1)

            logging.info(geocoded_unmatched.columns)
            
            adds_for_book = geocoded_unmatched.loc[
            geocoded_unmatched['latitude'].notnull(),
            ['address_full',
            'city',
            'latitude',
            'longitude',
            'state',
            'street_fraction',
            'street_name',
            'street_no',
            'street_pre_direction',
            'street_suffix',
            'uid',
            'zip',
            'zip_short'
            ]]

            adds_book_dedupe = adds_for_book.drop_duplicates(subset=['address_full',
                'city',
                'state',
                'zip_short'])

            logging.info(f"Adding {adds_book_dedupe.shape[0]} new locations to address book")
            add_book_new = pd.concat([add_book,
                adds_book_dedupe],
                ignore_index=True)

            # Deduplicating just in case
            # Weird things happen sometimes
            add_book_new = add_book_new.drop_duplicates(subset=['uid'])

            logging.info('Writing new address book')

            general.pos_write_csv(
                add_book_new,
                f"{conf['prod_data_dir']}/ttcs_address_book.csv")

            logging.info(f"Writing file with {geocoded_all.shape[0]} rows.")

            general.pos_write_csv(
                geocoded_all,
                geocoded_active,
                date_format="%Y-%m-%d")

    return "Successfully geocoded all businesses."



#: DAG function
def make_prod_files(**context):
    """Create subsets of active businesses based on create year."""
    
    df = pd.read_csv(geocoded_active,
                     low_memory=False)
    logging.info(f"Starting with {df.shape[0]} rows")

    df['account_status'] = df['account_status'].apply(lambda x: x.capitalize())

    logging.info('Renaming columns')
    df = df.rename(columns={'address_dt':'address_active_dt',
            'cert_exp_dt':'date_cert_expiration',
            'cert_eff_dt':'date_cert_effective',
            'creation_dt':'date_account_creation',
            'dba_name_dt':'dba_name_active_dt',
            'bus_start_dt': 'date_business_start',
            'latitude':'lat',
            'longitude':'lng',
            'apt_suite':'address_suite',
            'street_no':'address_no',
            'street_pre_direction':'address_pd',
            'street_name':'address_road',
            'street_suffix':'address_sfx',
            'street_fraction':'address_no_fraction',
            'city':'address_city',
            'state':'address_state',
            'zip':'address_zip',
            'pmb_box':'address_pmb_box',
            'po_box':'address_po_box',
            'primary_naics':'naics_code',
            'phone_full':'phone'
            })

    public_cols = ['account_key',
               'account_status',
               'account_status_code',
               'date_account_creation',
               'date_cert_expiration',
               'date_cert_effective',
               'business_owner_name',
               'ownership_type',
               'date_business_start',
               'dba_name',
               'naics_sector',
               'naics_code',
               'naics_description',
               'address_no',
               'address_pd',
               'address_road',
               'address_sfx',
               'address_no_fraction',
               'address_city',
               'address_state',
               'address_zip',
               'address_suite',
               'address_pmb_box',
               'address_po_box',
               'bid',
               'council_district',
               'lat',
               'lng']

    internal_cols = ['num_employees',
                 'home_based_ind',
                 'do_not_publish_ind',
                 'origin',
                 'fee_status',
                 'loc_override',
                 'phone',
                 'email',
                 'online_billing_email'
                 ]

    all_cols = public_cols+internal_cols

    df_public = df[df['do_not_publish_ind'] == 'N']
    logging.info(f"Public set has {df_public.shape[0]} rows")
    
    df_public_active = df_public[df_public['account_status_code'].isin(["A","P","I"])]
    logging.info(f"Public active set has {df_public_active.shape[0]} rows")
    
    df_public_inactive = df_public[df_public['account_status_code'].isin(["C","N"])]
    logging.info(f"Public inactive set has {df_public_inactive.shape[0]} rows")

    # Write Shop Local output
    general.pos_write_csv(
            df_public_active[all_cols],
            f"{conf['prod_data_dir']}/shop_local_businesses.csv",
            date_format="%Y-%m-%d")

    # Write Snowflake output
    general.sf_write_csv(df[all_cols],'tax_certs')
    
    # Start outputting Open Data sets
    general.pos_write_csv(
            df_public_active[public_cols],
            f"{conf['prod_data_dir']}/sd_businesses_active_datasd.csv",
            date_format="%Y-%m-%d")

    df_public_inactive['create_yr'] = pd.to_datetime(df_public_inactive['date_account_creation'], 
        errors='coerce').dt.year

    row_counts = 0

    # Really old certs, pre 1990
    od_inactive_hist = df_public_inactive.loc[
        (df_public_inactive['create_yr'] < 1990)]

    row_counts += od_inactive_hist.shape[0]
    
    general.pos_write_csv(
            od_inactive_hist,
            f"{conf['prod_data_dir']}/sd_businesses_pre1990_datasd.csv",
            date_format="%Y-%m-%d") 

    # Certs in 3 10-yr sets
    i = 1990
    while i < 2010:
        inactive_split = df_public_inactive.loc[
        (df_public_inactive['create_yr'] >= i) & 
        (df_public_inactive['create_yr'] < i+10)]

        row_counts += inactive_split.shape[0]

        general.pos_write_csv(
            inactive_split,
            f"{conf['prod_data_dir']}/sd_businesses_{i}to{i+10}_datasd.csv",
            date_format="%Y-%m-%d")        

        i += 10

    # More recent inactive certs in a 5-year set
    
    od_inactive_2010 = df_public_inactive.loc[
        (df_public_inactive['create_yr'] >= 2010) &
        (df_public_inactive['create_yr'] < 2015)]

    row_counts += od_inactive_2010.shape[0]

    general.pos_write_csv(
            od_inactive_2010,
            f"{conf['prod_data_dir']}/sd_businesses_2010to2015_datasd.csv",
            date_format="%Y-%m-%d")

    # Most recent inactive certs

    od_inactive_2015 = df_public_inactive.loc[
        (df_public_inactive['create_yr'] >= 2015)]

    row_counts += od_inactive_2015.shape[0]

    logging.info(f"All splits add up to {row_counts} rows")

    general.pos_write_csv(
            od_inactive_2015,
            f"{conf['prod_data_dir']}/sd_businesses_inactive_2015tocurr_datasd.csv",
            date_format="%Y-%m-%d")

    return "Successfully generated production files."

def send_arcgis():
    """ Upload shop local files to arcgis portal """

    conn = BaseHook.get_connection(conn_id="ARC_PORTAL")
    password = unquote(conn.password)
    conn_host = f"https://{conn.host}/{conn.schema}"

    arc_gis = GIS(conn_host,conn.login,password)

    biz_path = f"{conf['prod_data_dir']}/shop_local_businesses.csv"
    pin_path = f"{conf['prod_data_dir']}/ttcs-pins.csv"

    biz_props = {'title':'Shop Local Businesses',
    'description':'Business for publishing on Shop Local from BTC data',
    'tags':'Shop Local, GIS Dev',
    'overwrite':'true'
    }
    pin_props = {'title':'Business Tax Identification',
    'description':'Identification for Businesses displayed on Shop Local from BTC data',
    'tags':'Shop Local, GIS Dev',
    'overwrite':'true'
    }

    arc_gis.content.add(item_properties=biz_props,data=biz_path)
    arc_gis.content.add(item_properties=pin_props,data=pin_path)

    return "Successfully uploaded files to arcgis portal"
