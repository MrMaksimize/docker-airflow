"""Special Events _jobs file."""
import pandas as pd
import logging
import time
from airflow.hooks.mssql_hook import MsSqlHook
from poseidon.util import general
from poseidon.util import geospatial

conf = general.config


temp_file = conf['temp_data_dir'] + '/special_events.csv'
prod_file = conf['prod_data_dir'] + '/special_events_list_datasd.csv'


def get_special_events():
    """Get special events from DB."""
    se_query = general.file_to_string('./sql/special_events.sql', __file__)
    se_conn = MsSqlHook(mssql_conn_id='special_events_sql')
    df = se_conn.get_pandas_df(se_query)
    df['event_id'] = pd.to_numeric(
        df['event_id'], errors='coerce', downcast='integer')

    general.pos_write_csv(
        df, temp_file, date_format=conf['date_format_ymd_hms'])

    return "Retrieved special events to temp file."


def process_special_events():
    """Processe special events, including geocoding."""
    temp_df = pd.read_csv(temp_file, parse_dates=['event_start', 'event_end'])

    prod_df = pd.read_csv(prod_file, parse_dates=['event_start', 'event_end'])

    add_book = prod_df[['event_address', 'latitude', 'longitude']]
    add_book = add_book.dropna(axis=0).drop_duplicates().reset_index(drop=True)

    # typos fix
    temp_df['event_address'].replace('Streeet', "street")

    temp_df['latitude'] = ''
    temp_df['longitude'] = ''

    for index, row in temp_df.iterrows():
        address = row['event_address']

        if pd.isnull(address):
            temp_df.set_value(index, 'latitude', 'NaN')
            temp_df.set_value(index, 'longitude', 'NaN')

        elif address in add_book.event_address.values:
            lat = add_book[add_book['event_address'] == address].iloc[0]['latitude']
            lon = add_book[add_book['event_address'] == address].iloc[0]['longitude']
            temp_df.set_value(index, 'latitude', lat)
            temp_df.set_value(index, 'longitude', lon)

        else:
            coords = geospatial.geocode_address_google(address_line=address)

            lat = coords[0]
            lon = coords[1]

            temp_df.set_value(index, 'latitude', lat)
            temp_df.set_value(index, 'longitude', lon)

    updated_df = temp_df.sort_values(by='event_start', ascending=False)

    general.pos_write_csv(
        updated_df, prod_file, date_format=conf['date_format_ymd_hms'])

    return "Successfully generated special events prod file"
