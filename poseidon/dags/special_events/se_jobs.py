"""Special Events _jobs file."""
import pandas as pd
import logging
import time
import re
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general
from trident.util import geospatial
from airflow.models import Variable

conf = general.config


temp_file = conf['temp_data_dir'] + '/special_events.csv'
prod_v1_file = conf['prod_data_dir'] + '/special_events_list_datasd_v1.csv'
prod_file = conf['prod_data_dir'] + '/special_events_list_datasd.csv'
geocoded_addresses = 'events_address_book.csv'

def spell_number(s):
    """ Function to convert number to word """ 
    n = int(s)
    if n > 2 and n <= 10:
        if n == 3:
            return 'Third'
        elif n == 4:
            return 'Fourth'
        elif n == 5:
            return 'Fifth'
        elif n == 6:
            return 'Sixth'
        elif n == 7:
            return 'Seventh'
        elif n == 8:
            return 'Eighth'
        elif n == 9:
            return 'Ninth'
        elif n == 10:
            return 'Tenth'
        else:
            return s
    else:
        return s

def normalize_number_streets(addr_str):
    """ Function to fix numbered streets in downtown SD """
    fixed_address = addr_str    
    number_street_regex = re.compile(r'([0-9]+)(?=[sStTrR][TtHhdD])')
    find_number = re.search(number_street_regex,addr_str)
    if find_number:
        new_street = spell_number(find_number[0])
        if new_street != find_number[0]:
            fixed_address = re.sub(r'([0-9]+)([sStTrRnN][TtHhdD])',new_street,fixed_address)
        
    return fixed_address

def add_missing_suffix(addr_list):
    """ Function to add suffix for intersect streets """

    streets_regex = re.compile(r'\b[Ss][Tt][Rr][eE]+[Tt][Ss]\b')
    two_streets = re.search(streets_regex,addr_list[1])
    if two_streets:
        addr_list[0] = f"{addr_list[0]} St"
        return " & ".join(x.strip() for x in addr_list)
    else:
        return " & ".join(x.strip() for x in addr_list)

def normalize_address(address_string):
    """ Function to normalize known address errors to improve geocoding """
    
    if '&' in address_string or ' and ' in address_string:
        if '&' in address_string:
            address_list = address_string.split('&')
        else:
            address_list = address_string.split('and')
        added_suffix = add_missing_suffix(address_list)
        final_address = added_suffix.replace('&','and')
    else:
        final_address = address_string

    fixed_numbers = normalize_number_streets(final_address)

    fixed_suffix = geospatial.normalize_suffixes(fixed_numbers)

    return fixed_suffix

def get_special_events():
    """Get special events from DB."""
    se_query = general.file_to_string('./sql/special_events.sql', __file__)
    se_conn = MsSqlHook(mssql_conn_id='SPECIAL_EVENTS_SQL')
    df = se_conn.get_pandas_df(se_query)
    df['event_id'] = pd.to_numeric(
        df['event_id'], errors='coerce', downcast='integer')

    general.pos_write_csv(
        df, temp_file, date_format="%Y-%m-%d %H:%M:%S")

    return "Retrieved special events to temp file."


def process_special_events():
    """Process special events, including geocoding."""
    temp_df = pd.read_csv(temp_file, parse_dates=['event_start', 'event_end'])

    temp_df = temp_df.rename(columns={'event_start':'date_event_start',
        'event_end':'date_event_end',
        'event_address':'address_full'
        })

    logging.info("Reading in address book")
    bucket_name=Variable.get('S3_REF_BUCKET')
    s3_url = f"s3://{bucket_name}/reference/{geocoded_addresses}"
    add_book = pd.read_csv(s3_url,low_memory=False)

    add_book_join = pd.merge(temp_df,
        add_book,
        how='left',
        left_on='address_full',
        right_on='address_full',
        indicator=True
        )

    logging.info("Isolating new addresses not in address book")
    new_geocodes = add_book_join.loc[add_book_join['_merge'] == 'left_only']
    old_geocodes = add_book_join.loc[add_book_join['_merge'] == 'both']
    to_geocode = new_geocodes.loc[:,['address_full']].dropna().drop_duplicates().reset_index(drop=True)
    
    if to_geocode.empty:
        logging.info("There is nothing new to geocode")
        final_events = add_book_join.drop(columns=['_merge','address_normal'])
    else:
        logging.info(f"Need to geocode {to_geocode.shape[0]}")

        logging.info("Normalizing addresses for geocoding")
        geocode_addresses = to_geocode.apply(lambda x: normalize_address(x['address_full']),axis=1)
        to_geocode['address_normal'] = geocode_addresses

        geocoder_results = to_geocode.apply(lambda x: geospatial.census_address_geocoder(address_line=x['address_normal'],bounds='yes'), axis=1)
        coords = geocoder_results.apply(pd.Series)
        fresh_geocodes = to_geocode.assign(lat=coords[0],lng=coords[1])
        logging.info("Adding new geocodes to address book and data")

        new_add_book = pd.concat([add_book,fresh_geocodes],ignore_index=True)
        general.pos_write_csv(
            new_add_book,
            f"{conf['prod_data_dir']}/events_address_book.csv")

        new_geocodes = new_geocodes.drop(columns=['_merge','address_normal','lat','lng'])
        old_geocodes = old_geocodes.drop(columns=['_merge','address_normal'])

        new_events = pd.merge(new_geocodes,
            fresh_geocodes[['address_full','lat','lng']],
            left_on='address_full',
            right_on='address_full',
            how='left'
            )

        final_events = pd.concat([old_geocodes,new_events],ignore_index=True)

    updated_df = final_events.sort_values(by='date_event_start', ascending=False)

    logging.info('Writing final dataset')

    general.pos_write_csv(
        updated_df,
        prod_file,
        date_format="%Y-%m-%d %H:%M:%S")

    general.pos_write_csv(
        updated_df,
        prod_v1_file,
        date_format="%Y-%m-%d %H:%M:%S")

    return "Successfully generated special events prod file"
