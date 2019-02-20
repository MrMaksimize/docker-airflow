"""_jobs file for 'HistoricDistricts' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'HistoricDistricts'
prod_dir = conf['prod_data_dir']
layername = 'historic_districts_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('shortname', 'str:20'),
        ('sitename', 'str:40'),
        ('survey', 'str:50')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                      where="TYPE = 'Existing'")

    logging.info('Processing {layername} df.'.format(layername=layername))
    df = df.rename(columns={'historicsurvey': 'survey'})
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
