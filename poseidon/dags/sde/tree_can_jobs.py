"""_jobs file for tree canopy layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'ECO_TCANOPY_2014_SANDIEGO'
prod_dir = conf['prod_data_dir']
layername = 'tree_canopy_datasd'
layer = prod_dir + '/' + layername
prod_file = f"{prod_dir}/tree_canopy_tab_datasd.csv"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('treecanopy', 'int:5')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)

    logging.info('Processing {layername} df.'.format(layername=layername))

    logging.info('Converting {layername} df to shapefile.'.format(
        layername=layername))
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    logging.info('Writing records to table')

    general.pos_write_csv(df, prod_file)

    return 'Successfully converted {layername} to shapefile.'.format(
           layername=layername)
