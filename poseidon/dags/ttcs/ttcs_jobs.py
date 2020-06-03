"""This module contains jobs for extracting data out of TTCS."""

import os
import logging
import pandas as pd
import cx_Oracle
import string
import datetime as dt
import numpy as np

from trident.util import general
from trident.util import geospatial

conf = general.config
credentials = general.source['ttcs']

temp_all = conf['temp_data_dir'] + '/ttcs_all.csv'
clean_all = conf['temp_data_dir'] + '/ttcs_all_clean.csv'
geocoded_active = conf['temp_data_dir'] + '/ttcs_all_geocoded.csv'
bids_all = conf['temp_data_dir'] + '/ttcs_all_bids.csv'
geocoded_addresses = 'https://datasd-reference.s3.amazonaws.com/ttcs_address_book.csv'

curr_yr = dt.datetime.today().year

def get_active_businesses():
    """Query DB for 'Active Businesses' and save data to temp."""
    logging.info('Retrieving business tax license data')
    db = cx_Oracle.connect(credentials)
    sql = general.file_to_string('./sql/ttcs_biz.sql', __file__)
    df = pd.read_sql_query(sql, db)
    df_rows = df.shape[0]
    logging.info(f'Query returned {df_rows} results')
    general.pos_write_csv(
        df,
        temp_all,
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully retrieved active businesses data.'

def clean_data():
    """Clean business license data coming from TTCS."""
    logging.info('Reading query output')
    df = pd.read_csv(temp_all, 
        low_memory=False,
        )    
    df.columns = [x.lower() for x in df.columns]

    logging.info('Creating NAICS sector')

    df['naics_sector'] = df['naics_code'].apply(lambda x: str(x)[:2])

    logging.info('Extracting years for filter')

    df['bus_start_yr'] = pd.to_datetime(
        df['bus_start_dt'], errors='coerce').dt.year
    df['create_yr'] = pd.to_datetime(
        df['creation_dt'], errors='coerce').dt.year

    df_rows = df.shape[0]

    logging.info(f'Processed {df_rows} businesses')

    logging.info('Sorting by dba name active date')

    df_sort = df.sort_values(['account_key',
        'dba_name_dt',
        'address_dt'],
        ascending=[True,False,False]
        )

    logging.info('Deduping on account key, keeping latest dba and address')

    df_dedupe = df_sort.drop_duplicates(['account_key'])

    total_rows = df_dedupe.shape[0]

    logging.info(f'Deduped for {total_rows} total records')

    df_dedupe = df_dedupe.sort_values(by=['account_key',
        'creation_dt'],
        ascending=[True,
        False])

    logging.info('Writing final data to csv')

    general.pos_write_csv(
        df_dedupe,
        clean_all,
        date_format=conf['date_format_ymd'])
    return 'Successfully cleaned TTCS data.'

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

    logging.info('Get address book')
    add_book = pd.read_csv(geocoded_addresses,
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
                date_format=conf['date_format_ymd'])

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

        if to_geocode.empty:

            logging.info('Nothing to geocode')

            logging.info('Write final file')

            add_merge = add_merge.drop(['_merge'],axis=1)

            general.pos_write_csv(
                add_merge,
                geocoded_active,
                date_format=conf['date_format_ymd'])

        else:

            geocode_address = to_geocode[['street_no',
            'street_pre_direction',
            'street_name',
            'street_suffix']].apply(lambda x: ' '.join(x), axis=1)
            
            zip_split = to_geocode['zip'].str.split('-',expand=True)
            
            to_geocode = to_geocode.assign(address_full=geocode_address,zip_short=zip_split[0])
            
            geocode_dedupe = to_geocode.drop_duplicates(subset=['address_full',
                'city',
                'state',
                'zip_short'])
                        
            logging.info(f'Need to geocode {geocode_dedupe.shape[0]}')
        
            geocoder_results = geocode_dedupe.apply(lambda x: geospatial.census_address_geocoder(address_line=x['address_full'],locality=x['city'],state=x['state'],zip=x['zip_short']), axis=1)

            logging.info('Adding new coords to the df')
            coords = geocoder_results.apply(pd.Series)
            fresh_geocodes = geocode_dedupe.assign(latitude=coords[0],longitude=coords[1])

            logging.info('Merging geocodes to geocode df')
            geocoded = pd.merge(to_geocode,
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

            logging.info("Merging geocoded df back to unmatched addresses")

            geocoded_unmatched = pd.merge(add_unmatched,
                geocoded[['uid','latitude','longitude','address_full','zip_short']],
                how='left',
                left_on='uid',
                right_on='uid'
                )

            logging.info("Concat addresses matched with address book and addresses geocoded")
            geocoded_all = pd.concat([add_matched,geocoded_unmatched],ignore_index=True,sort=True)
            geocoded_all = geocoded_all.drop(['_merge'],axis=1)
            
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

            logging.info('Writing new address book')

            general.pos_write_csv(
                add_book_new,
                f"{conf['prod_data_dir']}/ttcs_address_book.csv")

            logging.info(f"Writing file with {geocoded_all.shape[0]} rows.")

            general.pos_write_csv(
                geocoded_all,
                geocoded_active,
                date_format=conf['date_format_ymd'])

    return "Successfully geocoded all businesses."

def join_bids():
    """Spatially joins BIDs data to active businesses data."""
    bids_geojson = conf['prod_data_dir'] + '/bids_datasd.geojson'
    active_bus_bid = geospatial.spatial_join_pt(geocoded_active,
                                                bids_geojson,
                                                lat='latitude',
                                                lon='longitude')

    general.pos_write_csv(
        active_bus_bid,
        bids_all,
        date_format=conf['date_format_ymd'])

    return "Successfully joined BIDs to active businesses"

def prod_files_prep(subset):

    df = subset.drop(['create_yr'],axis=1)
    df = df.sort_values(by=['account_key',
        'date_account_creation'],
        ascending=[True,
        False])
    return df


def make_prod_files():
    """Create subsets of active businesses based on create year."""
    df = pd.read_csv(bids_all,
                     low_memory=False,
                     parse_dates=['address_dt',
                                  'bus_start_dt',
                                  'cert_exp_dt',
                                  'creation_dt',
                                  'dba_name_dt'
                                  ])

    logging.info('Renaming columns')
    df = df.rename(columns={'address_dt':'address_active_dt',
            'cert_exp_dt':'date_cert_expiration',
            'creation_dt':'date_account_creation',
            'dba_name_dt':'dba_name_active_dt',
            'name':'bid',
            'apt_suite':'suite',
            'bus_start_dt': 'date_business_start',
            'latitude':'lat',
            'longitude':'lng',
            'street_no':'address_number',
            'street_pre_direction':'address_pd',
            'street_name':'address_road',
            'street_suffix':'address_sfx',
            'street_fraction':'address_number_fraction',
            'city':'address_city',
            'state':'address_state',
            'zip':'address_zip',
            'suite':'address_suite',
            'pmb_box':'address_pmb_box',
            'po_box':'address_po_box',
            })

    df.loc[((df['account_status'] == 'A') | 
        (df['account_status'] == 'P') | 
        (df['account_status'] == 'I')), 'account_status'] = "Active"

    df.loc[((df['account_status'] == 'C') | 
        (df['account_status'] == 'N')), 'account_status'] = "Inactive"

    df_prod = df[['account_key',
        'account_status',
        'date_account_creation',
        'date_cert_expiration',
        'business_owner_name',
        'ownership_type',
        'date_business_start',
        'dba_name',
        'naics_sector',
        'naics_code',
        'naics_description',
        'lat',
        'lng',
        'address_number',
        'address_pd',
        'address_road',
        'address_sfx',
        'address_number_fraction',
        'address_city',
        'address_state',
        'address_zip',
        'suite',
        'address_pmb_box',
        'address_po_box',
        'bid',
        'create_yr'
        ]]

    logging.info('Creating active subset')

    df_active = df_prod[df_prod['account_status'] == "Active"]

    active_rows = df_active.shape[0]

    logging.info(f'Found {active_rows} active businesses')

    logging.info('Writing active businesses set 1')

    active_pre07 = prod_files_prep(df_active[df_active['create_yr'] <= 2007])

    general.pos_write_csv(
        active_pre07,
        conf['prod_data_dir']+'/sd_businesses_active_pre08_datasd_v1.csv',
        date_format=conf['date_format_ymd'])

    active_pos07 = prod_files_prep(df_active[df_active['create_yr'] > 2007])

    general.pos_write_csv(
        active_pos07,
        conf['prod_data_dir']+'/sd_businesses_active_since08_datasd_v1.csv',
        date_format=conf['date_format_ymd'])

    df_inactive = df_prod[df_prod['account_status'] == "Inactive"].reset_index(drop=True)
    inactive_rows = df_inactive.shape[0]

    logging.info(f'Found {inactive_rows} inactive businesses')

    subset_no = np.ceil((curr_yr - 1990)/10.0)

    logging.info('Creating '+str(subset_no)+' subsets of inactive')

    sub_yr_start = 1990

    for i in range(subset_no.astype(int)):
        subset = df_inactive[
        (df_inactive['create_yr'] >= sub_yr_start) & 
        (df_inactive['create_yr'] <= sub_yr_start+9)]

        filename = str(sub_yr_start)+"to"+str(sub_yr_start+9)

        subset_prod = prod_files_prep(subset)

        logging.info('Writing file '+filename)
        general.pos_write_csv(
            subset_prod,
            f"{conf['prod_data_dir']}/sd_businesses_{filename}_datasd_v1.csv",
            date_format=conf['date_format_ymd'])

        sub_yr_start += 10

    return "Successfully generated production files."
