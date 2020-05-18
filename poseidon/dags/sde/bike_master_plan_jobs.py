"""_jobs file for 'bike master plan' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'BIKE_MASTER_PLAN'
prod_dir = conf['prod_data_dir']
layername = 'bike_master_plan_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:5'),
        ('class','int:2'),
        ('classtype','str:50')
    ])

gtype = 'LineString'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)
    #                                  where=" ")

    logging.info(f'Processing {layername} df.')
    # df = df.rename(columns={})
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
