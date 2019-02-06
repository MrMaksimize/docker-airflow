"""_jobs file for 'bike routes' sde extraction."""
from poseidon.util import general
from poseidon.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'BIKE_ROUTE'
prod_dir = conf['prod_data_dir']
layername = 'bike_routes_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:6'),
        ('rd20full', 'str:30'),
        ('max_elev', 'float:7.3'),
        ('route', 'int:2'),
        ('class', 'str:30')
    ])

gtype = 'LineString'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                     where="JURISDICTION = 'San Diego'")

    logging.info('Processing {layername} df.'.format(layername=layername))
    df = df.fillna('')
    df = df.rename(columns={'route_class_name': 'class'})

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
