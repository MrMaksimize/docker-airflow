"""_jobs file for 'transit routes' sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'TRANSIT_ROUTES_GTFS'
prod_dir = conf['prod_data_dir']
layername = 'transit_routes_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:6'),
        ('route_id', 'str:6'),
        ('shape_id', 'str:12'),
        ('rteshpname', 'str:20'),
        ('short_name', 'str:30'),
        ('long_name', 'str:50'),
        ('route_type', 'int:2'),
        ('rte_type_t', 'str:12'),
        ('agency_id', 'str:10'),
        ('route_url', 'str:100'),
        ('hex_color', 'str:6')
    ])

gtype = 'LineString'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)

    logging.info('Processing {layername} df.'.format(layername=layername))

    df = df.rename(columns={
        'routeshapename': 'rteshpname',
        'route_short_name': 'short_name',
        'route_long_name': 'long_name',
        'route_type_text': 'rte_type_t',
        'route_text_color': 'hex_color'})

    df = df.fillna('')

    logging.info('Converting {layername} df to shapefile.'.format(
        layername=layername))
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return 'Successfully converted {layername} to shapefile.'.format(
           layername=layername)
