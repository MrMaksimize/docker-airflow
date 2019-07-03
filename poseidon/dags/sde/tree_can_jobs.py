"""_jobs file for tree canopy layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging
import pymssql

conf = general.config
table = 'ECO_TCANOPY_2014_SANDIEGO'
prod_dir = conf['prod_data_dir']
layername = 'tree_canopy_datasd'
layer = prod_dir + '/' + layername
prod_file = f"{prod_dir}/tree_canopy_tab_datasd.csv"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('treecanopy', 'int:5')
    ])

gtype = 'Polygon'

def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    

    sde_server = conf['sde_server']
    sde_user = conf['sde_user']
    sde_pw = conf['sde_pw']

    sde_conn = pymssql.connect(sde_server, sde_user, sde_pw, 'sdw')
    query = 'SELECT *,'\
    + ' [Shape].STAsText() as geom, '\
    + ' [Shape].STArea() as geom_area'\
    + f' FROM SDW.CITY.{table}'

    logging.info(query)

    df = pd.read_sql(query, sde_conn)
    df.columns = [x.lower() for x in df.columns]

    logging.info('Processing {layername} df.'.format(layername=layername))

    logging.info('Converting {layername} df to shapefile.'.format(
        layername=layername))
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    logging.info('Writing records to table')

    general.pos_write_csv(df, prod_file)

    return 'Successfully converted {layername} to shapefile.'.format(
           layername=layername)

def shp_to_geojson():
    """Shapefile to GeoJSON."""
    cmd = geospatial.shp2geojsonOgr(layer)
    return cmd

def shp_to_topojson():
    """Shapefile to TopoJSON."""
    cmd = geospatial.shp2topojson(layer)
    return cmd

def geojson_to_geobuf():
    """Geojson to Geobuf."""
    geospatial.geojson2geobuf(layer)
    return 'Successfully converted geojson to geobuf.'


def geobuf_to_gzip():
    """Geobuf to gzip."""
    geospatial.geobuf2gzip(layername)
    return 'Successfully compressed geobuf.'


def shp_to_zip():
    """Shapefile to zip."""
    geospatial.shp2zip(layername)
    return 'Successfully transfered shapefiles to zip archive.'
