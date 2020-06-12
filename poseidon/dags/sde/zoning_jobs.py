"""_jobs file for 'zoning' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'ZONING_BASE_SD'
prod_dir = conf['prod_data_dir']
layername = 'zoning_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:6'),
        ('zone_name', 'str:12'),
        ('imp_date', 'str:10'),
        ('ordnum', 'str:10')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)

    logging.info(f'Processing {layername} df.')

    df = df.fillna('')
    df['imp_date'] = pd.to_datetime(df['imp_date'], errors='coerce')
    df['imp_date'] = df['imp_date'].dt.strftime(conf['date_format_ymd'])

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
