"""Geospatial Utilities."""
import os
import logging
from trident.util import general

import requests
import csv
import json
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import sjoin
from shapely.geometry import Point
from shapely.wkt import loads
from shapely.geometry import mapping
import fiona
from fiona import crs
import pymssql
import zipfile
from osgeo import ogr
from osgeo import osr
import geojson
import geobuf
import gzip
import shutil
import re
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
import datetime

conf = general.config

def census_address_geocoder(address_line='',
                           locality='San Diego',
                           state='CA',
                           zip='',
                           **kwargs):
    
    """Geocoding function using Census + Google"""
    logging.info(address_line)
    address_line = address_line.replace(' ','+')

    locality = locality.replace(' ','+')

    state = state.replace(' ','+')

    if zip != '':
        zip_append = '&zip='+zip
    else:
        zip_append = ''

    census_url = 'https://geocoding.geo.census.gov/geocoder/locations/address?'\
        + 'street={address}'\
        + '&city={locality}'\
        + '&state={state}'\
        + '{zip}'\
        + '&benchmark=4&format=json'

    census_url = census_url.format(address=address_line,
                     locality=locality,
                     state=state,
                     zip=zip_append)

    try:
        r = requests.get(census_url, timeout=10)
        body = json.loads(r.content)
        candidates = body['result']

        if candidates['addressMatches'] != []:
            coords = body['result']['addressMatches'][0]['coordinates']
            lat = pd.to_numeric(coords['y'],errors='coerce')
            lon = pd.to_numeric(coords['x'],errors='coerce')
            logging.info('Geocoded using Census')
            return lat, lon

        else:
            logging.info("Census result not found")
            logging.info("Trying Google")
            return google_address_geocoder(address_line=address_line,
                locality=locality,
                state=state,
                zip=zip,
                **kwargs)


    except Exception as e:
        logging.error(e)
        logging.info('Census geocoder failed, trying Google')
        return google_address_geocoder(address_line=address_line,
            locality=locality,
            state=state,
            zip=zip,
            **kwargs)

def google_address_geocoder(address_line='',
                           locality='San Diego',
                           state='CA',
                           zip='',
                           bounds='no',
                           **kwargs):
    """Geocoding function using just Google"""


    if zip != ' ':
        zip_append = '+'+zip
    else:
        zip_append = ''

    google_token = Variable.get("GOOGLE_TOKEN")

    if bounds == 'yes':

        google_url = 'https://maps.googleapis.com/maps/api/geocode/json?'\
            + 'address={address}'\
            + '+{locality}'\
            + '+{state}'\
            + '{zip}'\
            + '&bounds={bounds}'\
            + '&key={google_token}'

        google_url = google_url.format(address=address_line,
                         state=state,
                         locality=locality,
                         zip=zip_append,
                         google_token=google_token,
                         bounds='32.530161,-117.597986|33.511553,-116.080156')

    else:

        google_url = 'https://maps.googleapis.com/maps/api/geocode/json?'\
            + 'address={address}'\
            + '+{locality}'\
            + '+{state}'\
            + '{zip}'\
            + '&key={google_token}'

        google_url = google_url.format(address=address_line,
                         state=state,
                         locality=locality,
                         zip=zip_append,
                         google_token=google_token)

    try:
        r = requests.get(google_url, timeout=10)
        r.raise_for_status()
        body = json.loads(r.content)
        candidates = body['results']
        if candidates == []:
            logging.info("Google result not found")
            return np.nan, np.nan
        else:
            coords = body['results'][0]['geometry']['location']
            lat = pd.to_numeric(coords['lat'],errors='coerce')
            lon = pd.to_numeric(coords['lng'],errors='coerce')
            logging.info("Geocoded using Google")
            return lat, lon

    except Exception as e:
        logging.error(e)
        return np.nan, np.nan

def normalize_suffixes(address_str):

    street_regex = re.compile(r'\b[Ss][Tt][Rr]*[eE]*[Tt]*[Ss]*\b')
    avenue_regex = re.compile(r'\b[Aa][Vv][Ee][Nn]*[Uu]*[Ee]*\b')
    drive_regex = re.compile(r'\b[Dd][Rr][Ii]*[Vv]*[Ee]*\b')
    boulevard_regex = re.compile(r'\b[Bb][Oo]*[Uu]*[Ll][Ee]*[Vv][Aa]*[Rr]*[Dd]\b')
    way_regex = re.compile(r'\b[Ww][Yy]\b')
    road_regex = re.compile(r'\b[Rr][Oo][Aa][Dd]\b')
    circle_regex = re.compile(r'\b[Cc][Ii]*[Rr][Cc]*[Ll]*[Ee]*\b')
    parkway_regex = re.compile(r'\b[Pp][Aa][Rr][Kk][Ww][Aa][Yy]\b')
    
    fixed_address = re.sub(street_regex,'St',address_str)
    fixed_address = re.sub(avenue_regex,'Ave',fixed_address)
    fixed_address = re.sub(boulevard_regex,'Blvd',fixed_address)
    fixed_address = re.sub(drive_regex,'Dr',fixed_address)
    fixed_address = re.sub(way_regex,'Way',fixed_address)
    fixed_address = re.sub(road_regex,'Rd',fixed_address)
    fixed_address = re.sub(circle_regex,'Cir',fixed_address)
    fixed_address = re.sub(parkway_regex,'Pky',fixed_address)
    
    # Remove periods
    fixed_address = fixed_address.replace('.','')
    
    return fixed_address

def geocode_address_google(address_line='',
                           locality='San Diego',
                           state='CA',
                           **kwargs):
    """Geocoding function using Google geocoding API."""
    address_line = str(address_line)
    locality = str(locality)
    state = str(state)
    google_token = Variable.get("GOOGLE_TOKEN")
    url = 'https://maps.googleapis.com/maps/api/geocode/json?'\
          + 'address={address}&'\
          + 'components=country:US|'\
          + 'administrative_area:{state}|'\
          + 'locality:{locality}&'\
          + 'key={google_token}'

    url = url.format(address=address_line,
                     state=state,
                     locality=locality,
                     google_token=google_token)


    logging.info('Google Geocoding for: ' + address_line)
    if address_line in ['None', '', 'NaN', 'nan']:
        logging.info('No geocode for: ' + address_line)
        return None, None
    else:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            body = json.loads(r.content)
            candidates = body['results']
            if candidates == []:
                logging.info('No geocode for: ' + address_line)
                return None, None
            else:
                coords = body['results'][0]['geometry']['location']
                lat = coords['lat']
                lon = coords['lng']
                logging.info('Geocode success for: ' + address_line)
                return lat, lon
        except Exception as e:
            logging.error(e)
            return None, None


def reverse_geocode_google(lat='', lon='', **kwargs):
    """Reverse geocoding function using Google geocoding API."""
    google_token = Variable.get("GOOGLE_TOKEN")
    lat = str(lat)
    lon = str(lon)
    url = 'https://maps.googleapis.com/maps/api/geocode/json?'\
          + 'latlng={lat},{lon}&key={google_token}'
    url = url.format(lat=lat, lon=lon, google_token=google_token)
    if (lat == 'None' or lat == '' or lon == 'None' or lon == ''):
        logging.info('No reverse geocode for this entry')
        return None
    else:
        try:
            r = requests.get(url)
            r.raise_for_status()
            body = json.loads(r.content)
            candidates = body['results']
            if candidates == []:
                logging.info('No reverse geocode for: ' + lat + ', ' + lon)
                return None
            else:
                if 'formatted_address' not in candidates[0]:
                    logging.info('No reverse geocode for: ' + lat + ', ' + lon)
                    return None
                else:
                    address = candidates[0]['formatted_address']
                    logging.info('Geocode success for: ' + lat + ', ' + lon)
                    return address
        except Exception as e:
            logging.error(e)
            return None


def geocode_address_esri(address_line='', **kwargs):
    """Geocoding function using SANDAG geocoder."""
    # Type safe
    address_line = str(address_line)
    url = "http://gis1.sandag.org/sdgis/rest/services/REDI/REDI_COMPOSITE_LOC/GeocodeServer/findAddressCandidates"
    payload = {
        'City': 'San Diego',
        'SingleLine': address_line,
        'outSR': '4326',
        'f': 'pjson'
    }
    logging.info('ESRI Geocoding for: ' + address_line)
    if (address_line == 'None' or address_line == ''):
        logging.info('No geocode for: ' + address_line)
        return None, None
    else:
        r = requests.get(url, payload)
        r.raise_for_status()
        resp = r.json()
        candidates = resp['candidates']
        if candidates == []:
            logging.info('No geocode for: ' + address_line)
            return None, None
        else:
            logging.info('Geocode success for: ' + address_line)
            return candidates[0]['location']['y'],
            candidates[0]['location']['x']


def df_to_geodf_pt(df, lat='lat', lon='lon'):
    """Convert a dataframe with lat/lon (points) to a Geodataframe."""
    logging.info('Converting points df to geodf.')
    df = df[np.isfinite(df[lat])]
    df = df[np.isfinite(df[lon])]
    df = df[df[lat] != 0]
    df = df[df[lon] != 0]
    df['geometry'] = df.apply(lambda z: Point(z[lon], z[lat]), axis=1)
    gdf = gpd.GeoDataFrame(df)
    logging.info('Successfully created a geodf from points df.')
    return gdf


def geojson_to_geodf(file):
    """Open a geojson file and turn it into a GeodataFrame."""
    logging.info('Importing geojson file as geodf.')
    gdf = gpd.read_file(file)
    logging.info('Successfully imported geojson file as geodf.')
    return gdf


def spatial_join_pt(pt_file, poly_file, lat='lat', lon='lon'):
    """Spatially join polygon attributes to point data.

    'pt_file' is a csv file with latitude and longitude attributes that
    can be interpreted as points.

    'poly_file' is a geojson file that contains polygon data.

    lat --> latitude field in the point df
    lon --> longitude field in the point df

    Both layers must use the same CRS.

    This function returns a DataFrame, not a Geodataframe.
    """
    if isinstance(pt_file,str):

        logging.info('Loading point file')
        df = pd.read_csv(pt_file,low_memory=False)

    elif isinstance(pt_file,pd.DataFrame):

        logging.info('Point file already loaded')
        df = pt_file

    logging.info('Starting with {} rows in point file'.format(df.shape[0]))

    df = df.reset_index(drop=True)
    df_cols = df.columns.values.tolist()
    
    logging.info('Converting point file to geodf')
    pt = df_to_geodf_pt(df, lat, lon)
    logging.info('Loading poly file as geodf')
    poly = geojson_to_geodf(poly_file)
    pt.crs = poly.crs
    logging.info(f'Set point to {pt.crs} to match {poly.crs}')
    
    logging.info('Operating spatial join.')
    pt_join = sjoin(pt, poly, how='left')
    pt_join = pt_join.drop(['geometry', 'index_right'], axis=1)
    
    logging.info('Successfully spatially joined data.')
    join_cols = pt_join.columns.values.tolist()
    new_cols = [x for x in join_cols if x not in df_cols]

    # We will not keep the results for points 
    # that join to multiple polygons
    pt_join = pt_join.reset_index().drop_duplicates(subset="index",keep=False).set_index("index")
    
    # We must join the result back to original dataframe to keep all rows
    final = pd.merge(df,pt_join[new_cols],left_index=True,right_index=True,how="left")
    
    logging.info('Finished with {} rows'.format(final.shape[0]))
    return final


def extract_sde_data(table, where=''):
    """Extract table from SDE and return dataframe.

    'table': table name in SDE - what comes after 'SDW.CITY'.

    'where': where clause to refine results (e.g County scale datasets).

    """
    conn = BaseHook.get_connection(conn_id="SDE")
    sde_server = conn.host
    sde_user = conn.login
    sde_pw = conn.password
    sde_schema = conn.schema

    sde_conn = pymssql.connect(
        server=sde_server,
        port=1433,
        user=sde_user,
        password=sde_pw,
        database=sde_schema)

    if where == '':
        query = "SELECT *, [Shape].STAsText() as geom FROM SDW.CITY.{table}"
        query = query.format(table=table)

    else:
        query = "SELECT *, [Shape].STAsText() as geom FROM SDW.CITY.{table}" \
                + " WHERE {where}"
        query = query.format(table=table, where=where)

    df = pd.read_sql(query, sde_conn)

    date_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]']

    if len(date_cols) > 0:
        for dc in date_cols:
            df[dc] = df[dc].astype(str)

    logging.info(df.columns)
    df.columns = [x.lower() for x in df.columns]
    df = df.drop('shape', 1)
    return df

def df2shp(df, folder, layername, dtypes, gtype, epsg):
    """Convert a processed df to a shapefile.

    'df' is a dataframe.

    'folder' is the path to the folder where the shapefile will be saved.

    'layername' is the name of the shapefile.

    'dtypes' is an Orderdict containing the dtypes for each field.

    'gtype' is the geometry type.

    'epsg' is the EPSG code of the output.

    """
    schema = {'geometry': gtype, 'properties': dtypes}

    with fiona.collection(
        folder + '/' + layername + '.shp',
        'w',
        driver='ESRI Shapefile',
        crs=crs.from_epsg(epsg),
        schema=schema
    ) as shpfile:
        for index, row in df.iterrows():
            if row['geom'] != 'POINT EMPTY':
                geometry = loads(row['geom'])
                props = {}
                for prop in dtypes:
                    if type(row[prop]) is datetime.datetime:
                        props[prop] = row[prop].strftime('%Y-%m-%d %H:%M:%S')
                    elif type(row[prop]) is datetime.date:
                        props[prop] = row[prop].strftime('%Y-%m-%d')
                    else:
                        props[prop] = row[prop]
                shpfile.write({'properties': props, 'geometry': mapping(geometry)})

    return 'Extracted {layername} shapefile.'.format(layername=layername)


def shp2geojson(layer):
    """Shapefile to Geojson conversion using mapshaper."""
    cmd = 'mapshaper {layer}.shp'\
        + ' -proj wgs84'\
        + ' -o format=geojson precision=0.00000001'\
        + ' {layer}.geojson'

    cmd = cmd.format(layer=layer)

    return cmd

def shp2geojsonOgr(layer):
    """Shapefile to Geojson conversion using ogr."""
    cmd = 'ogr2ogr -f GeoJSON -t_srs'\
        + ' crs:84'\
        + ' {layer}.geojson'\
        + ' {layer}.shp'

    cmd = cmd.format(layer=layer)

    return cmd

def shp2kml(layer):
    """Shapefile to KML conversion using ogr."""
    # Rename fields to Name and Description
    # Or specify them in the command
    cmd = 'ogr2ogr -f KML'\
    + f' {layer}.kml'\
    + f' {layer}.shp'
    #+ ' -dsco NameField='\
    #+ ' DescriptionField='

    cmd = cmd.format(layer=layer)

    return cmd


def shp2topojson(layer):
    """Shapefile to TopoJSON conversion using mapshaper."""
    cmd = 'mapshaper {layer}.shp'\
        + ' -proj wgs84'\
        + ' -o format=topojson precision=0.00000001'\
        + ' {layer}.topojson'

    cmd = cmd.format(layer=layer)

    return cmd


def geojson2geobuf(layer):
    """Geojson to Geobuf conversion."""
    with open('{layer}.geojson'.format(layer=layer), 'r') as json:
        with open('{layer}.pbf'.format(layer=layer), 'wb') as buf:
            data = geojson.load(json)
            pbf = geobuf.encode(data)
            buf.write(pbf)
    return 'Successfully wrote geobuf.'


def geobuf2gzip(layername):
    """Gzip geobuf file."""
    os.chdir(conf['prod_data_dir'])
    with open('{layername}.pbf'.format(layername=layername), 'rb') as f_in:
        with gzip.open('{layername}.pbf.gz'.format(layername=layername), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.rename('{layername}.pbf.gz'.format(layername=layername),
              '{layername}.pbf'.format(layername=layername))
    os.chdir(os.environ.get("AIRFLOW_HOME", ""))
    return 'Sucessfully gzipped geobuf file.'


def shp2zip(layername):
    """Transfer shapefile component files to .zip archive."""
    home_dir = os.environ.get("AIRFLOW_HOME", "")
    os.chdir(conf['prod_data_dir'])
    list_files = [
        '{layername}.shp'.format(layername=layername),
        '{layername}.shx'.format(layername=layername),
        '{layername}.dbf'.format(layername=layername),
        '{layername}.prj'.format(layername=layername),
        '{layername}.cpg'.format(layername=layername)
    ]

    with zipfile.ZipFile('{layername}.zip'.format(layername=layername), 'w') as zipfolder:
        for file in list_files:
            zipfolder.write(file)
            os.remove(file)

    os.chdir(home_dir)

    return 'Successfully zipped shapefile.'


def pt_proj_conversion(lon, lat, in_proj=2230, out_proj=4326):
    """Convert a set of coordinates from one projection system to another."""
    source = osr.SpatialReference()
    source.ImportFromEPSG(in_proj)

    target = osr.SpatialReference()
    target.ImportFromEPSG(out_proj)

    transform = osr.CoordinateTransformation(source, target)

    point = ogr.CreateGeometryFromWkt("POINT ({lon} {lat})".format(lon=lon,
                                                                   lat=lat))

    point.Transform(transform)

    lat_t = point.GetY()
    lon_t = point.GetX()

    return lon_t, lat_t



def get_address_for_apn(apn):

    url = "https://gissd.sandag.org/rdw/rest/services/Parcel/Parcels/MapServer/1/query"

    querystring = {
        "where":"APN IN ('{}')".format(apn),
        #"where": where_stmt,
        "objectIds":"",
        "time":"",
        "geometry":"",
        "geometryType":"esriGeometryEnvelope",
        "inSR":"",
        "spatialRel":"esriSpatialRelIntersects",
        "distance":"",
        "units":"esriSRUnit_Foot",
        "relationParam":"",
        "outFields": "*",
        "returnGeometry":"true",
        "maxAllowableOffset":"",
        "geometryPrecision":"",
        "outSR":"4326",
        "gdbVersion":"",
        "returnDistinctValues":"false",
        "returnIdsOnly":"false",
        "returnCountOnly":"false",
        "returnExtentOnly":"false",
        "orderByFields":"",
        "groupByFieldsForStatistics":"",
        "outStatistics":"",
        "returnZ":"false",
        "returnM":"false",
        "multipatchOption":"",
        "resultOffset":"",
        "resultRecordCount":"",
        "f":"json"}

    headers = {
        'Cache-Control': "no-cache",
        'Postman-Token': "45d06817-feae-4fec-8dff-71c088d518d7"
        }

    logging.info("Get address for APN {}".format(apn))

    response = requests.request("POST", url, headers=headers, params=querystring)
    data = response.json()


    if response.status_code == requests.codes.ok:
        apn_info = data['features'][0]['attributes']
        return "{} {} {}".format(apn_info['SITUS_ADDRESS'], apn_info['SITUS_STREET'], apn_info['SITUS_SUFFIX'])
    else:
        return f"APN: {apn}"
