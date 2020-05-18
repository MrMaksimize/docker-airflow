"""_jobs file for 'council districts' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'COUNCIL'
prod_dir = conf['prod_data_dir']
layername = 'council_districts_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:1'),
        ('district', 'int:1'),
        ('name', 'str:20'),
        ('phone', 'str:20'),
        ('website', 'str:60'),
        ('perimeter', 'float:10.3'),
        ('area', 'float:15.3')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)

    logging.info(f'Processing {layername} df.')

    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
