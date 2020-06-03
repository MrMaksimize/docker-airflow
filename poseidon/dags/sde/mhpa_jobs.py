"""_jobs file for 'mhpa' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'MHPA'
prod_dir = conf['prod_data_dir']
layername = 'mhpa_areas_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:5'),
        ('habpres', 'int:3'),
        ('subarea', 'int:4'),
        ('linestatus', 'int:2'),
        ('inhabpres', 'str:3'),
        ('acres', 'float:38.8')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)
    #                                  where=" ")

    logging.info(f'Processing {layername} df.')
    #df = df.rename(columns={})
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
