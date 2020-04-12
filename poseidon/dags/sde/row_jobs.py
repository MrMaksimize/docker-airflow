"""_jobs file for 'right of way' sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'RIGHT_OF_WAY'
prod_dir = conf['prod_data_dir']
layername = 'right_of_way_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:6'),
        ('parcelid', 'int:8'),
        ('pending', 'str:1'),
        ('multi', 'str:1'),
        ('sub_type', 'int:1'),
        ('adddate', 'str:10'),
        ('postdate', 'str:10')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table,
                                     where="OVERLAY_JURIS = 'SD'")

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
