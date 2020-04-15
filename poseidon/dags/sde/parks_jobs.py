"""_jobs file for 'parks' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'PARKS_SD'
prod_dir = conf['prod_data_dir']
layername = 'parks_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('name', 'str:65'),
        ('gis_acres', 'float:38.8'),
        ('park_type','str:15'),
        ('location','str:50')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table,
                                     where="OWNERSHIP = 'City of San Diego'"
                                     )

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
