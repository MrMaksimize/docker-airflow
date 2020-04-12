"""_jobs file for 'sdpd_divisions' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'LAW_BEATS'
prod_dir = conf['prod_data_dir']
layername = 'pd_beats_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:4'),
        ('beat', 'int:5'),
        ('div', 'int:10'),
        ('serv', 'int:10'),
        ('name', 'str:60')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table,
                                      where="AGENCY = 'SD'")

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
