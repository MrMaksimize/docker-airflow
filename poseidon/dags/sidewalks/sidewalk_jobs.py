"""_jobs file for sidewalk oci."""
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import geopandas
from airflow.hooks.mssql_hook import MsSqlHook
from poseidon.util import general
from poseidon.util import geospatial
from collections import OrderedDict
import pymssql


conf = general.config

cond_file = conf['prod_data_dir'] + '/sidewalk_cond_datasd.csv'
layername = 'sidewalks'
prod_dir = conf['prod_data_dir']
dtypes = OrderedDict([
        ('seg_id', 'str'),
        ('geojoin_id','str'),
        ('fun_loc_id','str'),
        ('loc_desc', 'str'),
        ('xstrt1', 'str'),
        ('xstrt2', 'str'),
        ('strt_side', 'str'),
        ('council', 'int'),
        ('comm_plan', 'int'),
        ('material','str'),
        ('width', 'float'),
        ('oci_yr', 'int'),
        ('oci_desc', 'str'),
        ('oci', 'float')
    ])

gtype = 'LineString'

path_to_file = conf['prod_data_dir'] + '/' + 'sidewalks'
datasd_name = 'sidewalks'

def get_sidewalk_data(**kwargs):
    """Get sidewalk condition data from DB."""
    sw_query = general.file_to_string('./sql/sidewalk_insp.sql', __file__)
    sw_conn = MsSqlHook(mssql_conn_id='streets_cg_sql')

    df = sw_conn.get_pandas_df(sw_query)

    # Rename columns we're keeping
    df = df.rename(columns={
        'sap_id': 'seg_id',
        'legacy_id': 'geojoin_id',
        'inspectiondate': 'oci_date',
        'rating': 'oci_desc',
        'condition': 'oci'
        })

    df = df.drop(['cgLastModified',
        'MaxInspect',
        'MaxMod'],axis=1)

    # Write csv
    logging.info('Writing ' + str(df.shape[0]))
    
    general.pos_write_csv(
       df, cond_file, date_format=conf['date_format_ymd'])
    
    return "Successfully wrote prod file"

def get_sidewalk_gis(**kwargs):
    """ Get sidewalk geodatabase from shared drive"""

    sde_server = conf['sde_server']
    sde_user = conf['sde_user']
    sde_pw = conf['sde_pw']

    sde_conn = pymssql.connect(sde_server, sde_user, sde_pw, 'sdw')
    query = "SELECT *, [Shape].STAsText() as geom FROM SDW.IAMSD.SIDEWALK"
    
    df = pd.read_sql(query, sde_conn)
    
    df.columns = [x.lower() for x in df.columns]
    
    df = df.drop('shape', 1)

    df = df.rename(columns={'sapid':'seg_id',
        'cdcode':'council',
        'cpcode':'comm_plan',
        'legacy_id':'geojoin_id',
        'iamfloc':'fun_loc_id',
        'loc_descr':'loc_desc'
        })

    df = df.fillna('')

    oci = pd.read_csv(conf['prod_data_dir'] + '/sidewalk_cond_datasd.csv')
    oci['oci_yr'] = oci['oci_date'].map(lambda x: pd.to_datetime(x).year)

    df_merge = pd.merge(df,
        oci,
        how="left",
        left_on=['seg_id','geojoin_id'],
        right_on=['seg_id','geojoin_id']
        )

    rows = df_merge.shape[0]

    logging.info('processed {} rows'.format(rows))

    geospatial.df2shp(df=df_merge,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)

    return "Converted table to shapefile"

def shp_to_geojson():
    """Shapefile to GeoJSON."""
    cmd = geospatial.shp2geojson(path_to_file)
    return cmd

def shp_to_topojson():
    """Shapefile to TopoJSON."""
    cmd = geospatial.shp2topojson(path_to_file)
    return cmd

def geojson_to_geobuf():
    """Geojson to Geobuf."""
    geospatial.geojson2geobuf(path_to_file)
    return 'Successfully converted geojson to geobuf.'


def geobuf_to_gzip():
    """Geobuf to gzip."""
    geospatial.geobuf2gzip(datasd_name)
    return 'Successfully compressed geobuf.'


def shp_to_zip():
    """Shapefile to zip."""
    geospatial.shp2zip(datasd_name)
    return 'Successfully transfered shapefiles to zip archive.'
