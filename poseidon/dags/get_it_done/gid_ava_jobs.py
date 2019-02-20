"""Get it Done - Abandoned Vehicles _jobs file."""
import os
import string
import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin
from shapely.geometry import Point
import fiona.crs
import logging
from trident.util import general
from trident.util.sf_client import Salesforce

conf = general.config

temp_file_ava = conf['temp_data_dir'] + '/gid_ava_temp.csv'
prod_file_ava = conf['prod_data_dir'] + '/gid_ava_yesterday_datasd.csv'


def get_sf_gid_ava():
    """Get abandoned vehicles for yesterday from SF."""
    username = conf['mrm_sf_user']
    password = conf['mrm_sf_pass']
    security_token = conf['mrm_sf_token']

    query_string = general.file_to_string('./sql/gid_ava.sql', __file__)

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull records
    logging.info('Pull records from SF')

    result = sf.get_query_all(query_string)

    records = result['records']

    rows = []
    cols = ['case_number', 'created_date', 'description', 'SAP_url', 'vehicle_type',
            'vehicle_color', 'vehicle_plate', 'vehicle_state', 'lat',
            'lon', 'address', 'contact_fname', 'contact_lname',
            'contact_phone', 'contact_email']

    for record in records:
        case_number = record['CaseNumber']
        created_date = general.utc_to_pst(
            timestamp_str=record['CreatedDate'],
            in_fmt='%Y-%m-%dT%H:%M:%S.000+0000',
            out_fmt='%Y-%m-%d %H:%M:%S')
        description = record['Description']
        sap_url = record['SAP_URL__c']
        vehicle_type = record['Answer_1__c']
        vehicle_color = record['Answer_2__c']
        vehicle_plate = record['Answer_3__c']
        vehicle_state = record['Answer_4__c']
        lat = record['Geolocation__Latitude__s']
        lon = record['Geolocation__Longitude__s']
        address = record['Street_Address__c']
        try:
            fname = record['Contact']['FirstName']
        except:
            fname = 'NaN'
        try:
            lname = record['Contact']['LastName']
        except:
            lname = 'NaN'
        try:
            phone = record['Contact']['Phone']
        except:
            phone = 'NaN'
        try:
            email = record['Contact']['Email']
        except:
            email = 'NaN'

        row = (case_number, created_date, description, sap_url, vehicle_type,
               vehicle_color, vehicle_plate, vehicle_state, lat, lon,
               address, fname, lname, phone, email)

        rows.append(row)

    df = pd.DataFrame(data=rows, columns=cols)

    general.pos_write_csv(df, temp_file_ava)

    return "Successfully retrieved abandoned vehicles data"


def wget_police_beats():
    """Retieve police beats shapefile."""
    wget_str = "wget -np --continue " \
        + "--directory-prefix=$temp_dir " \
        + "http://seshat.datasd.org/pd/police_beats_datasd.geojson"
    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
        temp_dir=conf['temp_data_dir']
    )

    return command


def join_police_beats():
    """Join police beats to abandoned vehicles data."""
    logging.info('Preparing data for spatial joins.')
    # Beats to gdf
    beats_geojson = conf['temp_data_dir'] + '/police_beats_datasd.geojson'
    beats_gdf = gpd.read_file(beats_geojson)

    # AVA to gdf
    ava_df = pd.read_csv(temp_file_ava)
    ava_df = ava_df[np.isfinite(ava_df['lat'])]
    ava_df = ava_df[ava_df['lat'] != 0]
    ava_df['geometry'] = ava_df.apply(lambda z: Point(z.lon, z.lat), axis=1)
    ava_gdf = gpd.GeoDataFrame(ava_df)

    # Set CRS
    ava_gdf.crs = beats_gdf.crs

    logging.info('Proceeding to spatial joins.')

    # Proceed to spatial join
    join_beats = sjoin(ava_gdf, beats_gdf, how='left')
    join_beats = join_beats.drop(['geometry', 'index_right'], axis=1)
    join_beats = join_beats.sort_values(by='beat_code', ascending=True)

    general.pos_write_csv(join_beats, prod_file_ava)

    return "Successfully joined beats to abandoned vehicles data"
