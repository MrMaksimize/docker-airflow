"""_jobs file for 'BIDs' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'BUS_IMPROVEMENT_DISTRICTS'
prod_dir = conf['prod_data_dir']
layername = 'bids_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:3'),
        ('name', 'str:30'),
        ('long_name', 'str:200'),
        ('status', 'str:20'),
        ('link', 'str:200')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)

    logging.info('Processing {layername} df.'.format(layername=layername))
    df = df.rename(columns={'name': 'long_name',
                            'name2': 'name',
                            'type': 'status'})
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
