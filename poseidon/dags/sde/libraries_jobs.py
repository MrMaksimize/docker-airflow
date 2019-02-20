"""_jobs file for 'libraries' layer sde extraction."""
from poseidon.util import general
from poseidon.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'LIBRARY'
prod_dir = conf['prod_data_dir']
layername = 'libraries_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:2'),
        ('name', 'str:30'),
        ('address', 'str:30'),
        ('city', 'str:20'),
        ('zip', 'str:5'),
        ('phone', 'str:20'),
        ('website', 'str:50'),
        ('tb', 'str:12')
    ])

gtype = 'Point'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                     where="DISTRICT = 'San Diego Public (City)'")

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
