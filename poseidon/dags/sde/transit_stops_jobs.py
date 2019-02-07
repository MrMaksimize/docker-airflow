"""_jobs file for 'transit stops' layer sde extraction."""
from poseidon.util import general
from poseidon.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'TRANSIT_STOPS_GTFS'
prod_dir = conf['prod_data_dir']
layername = 'transit_stops_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:6'),
        ('stop_uid', 'str:20'),
        ('stop_id', 'str:10'),
        ('stop_code', 'int:6'),
        ('stop_name', 'str:30'),
        ('stop_lat', 'float:12.6'),
        ('stop_lon', 'float:12.6'),
        ('stop_agncy', 'str:10'),
        ('wheelchair', 'int:1'),
        ('intersec', 'str:20'),
        ('stop_place', 'str:20'),
        ('parent_sta', 'str:20')
    ])

gtype = 'Point'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)

    logging.info('Processing {layername} df.'.format(layername=layername))

    df = df.rename(columns={
            'stop_agency': 'stop_agncy',
            'wheelchair_boarding': 'wheelchair',
            'intersection_code': 'intersec',
            'parent_station': 'parent_sta'})

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
