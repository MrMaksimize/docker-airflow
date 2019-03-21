"""_jobs file for 'cmty_block_grants' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'CMTY_BLOCK_DEV_GRANTS'
prod_dir = conf['prod_data_dir']
layername = 'block_grants_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:5'),
        ('statefp', 'str:2'),
        ('countyfp', 'str:3'),
        ('tractce', 'str:6'),
        ('blkgrpce', 'str:1'),
        ('geoid', 'str:12'),
        ('geoname', 'str:255'),
        ('low','int:5'),
        ('lowmod', 'int:5'),
        ('lmmi', 'int:5'),
        ('lowmoduniv', 'int:5'),
        ('lowmod_pct', 'float:10.6'),
        ('low_pct', 'float:10.6'),
        ('lmmi_pct', 'float:10.6'),
        ('total_pop', 'int:5'),
        ('tot_units', 'int:5'),
        ('occ_units', 'int:4'),
        ('vac_units', 'int:3')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)
    #                                  where=" ")

    logging.info('Processing {layername} df.'.format(layername=layername))
    df = df.rename(columns={'sum_totalpop': 'total_pop',
                              'sum_totunits': 'tot_units',
                              'sum_occunits': 'occ_units',
                              'sum_vacunits': 'vac_units'})
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
