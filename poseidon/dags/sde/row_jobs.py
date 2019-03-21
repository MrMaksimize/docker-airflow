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
layer = prod_dir + '/' + layername

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
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                     where="OVERLAY_JURIS = 'SD'")

    logging.info('Processing {layername} df.'.format(layername=layername))
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
