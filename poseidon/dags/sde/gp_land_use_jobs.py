"""_jobs file for 'general plan land use' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'GENERAL_PLAN_LAND_USE'
prod_dir = conf['prod_data_dir']
layername = 'gp_land_use_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('plan_name', 'str:100'),
        ('dens_low', 'float:38.8'),
        ('dens_high', 'float:38.8'),
        ('dens_bonus', 'float:38.8'),
        ('gp_lu_desc', 'str:254'),
        ('chg_type', 'str:50'),
        ('chg_date', 'float:38.8')
        ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table)

    logging.info(f'Processing {layername} df.')

    df = df.rename(columns={
        'density_low': 'dens_low',
        'density_hi': 'dens_high',
        'density_bonus': 'dens_bonus',
        'change_type': 'chg_type',
        'change_date': 'chg_date'
    })
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
