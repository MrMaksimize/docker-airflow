"""_jobs file for 'fire stations' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'FIRE_STATION'
prod_dir = conf['prod_data_dir']
layername = 'fire_stations_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:2'),
        ('stat_name', 'str:10'),
        ('stat_type', 'str:10'),
        ('seed', 'str:1'),
        ('dist_name', 'str:60'),
        ('dispatch', 'str:1'),
        ('phone_num', 'str:15'),
        ('sta_num', 'int:3')
    ])

gtype = 'Point'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                     where="JURIS = 'SD'")

    logging.info('Processing {layername} df.'.format(layername=layername))

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
