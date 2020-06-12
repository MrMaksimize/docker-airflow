"""_jobs file for 'community plan' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'CMTY_PLAN'
prod_dir = conf['prod_data_dir']
layername = 'cmty_plan_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:3'),
        ('cpcode', 'int:3'),
        ('cpname', 'str:50'),
        ('acreage', 'float:12.5'),
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)

    logging.info(f'Processing {layername} df.')
    df['cpname'] = df['cpname'].str.title()
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
