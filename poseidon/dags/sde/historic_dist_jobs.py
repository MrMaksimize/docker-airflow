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
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('shortname', 'str:20'),
        ('sitename', 'str:40'),
        ('survey', 'str:50')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table,
                                      where="TYPE = 'Existing'")

    logging.info(f'Processing {layername} df.')
    df = df.rename(columns={'historicsurvey': 'survey'})
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
