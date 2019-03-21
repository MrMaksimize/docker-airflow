"""_jobs file for 'joint use' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'JOINTUSEPARKS'
prod_dir = conf['prod_data_dir']
layername = 'joint_use_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:5'),
        ('name', 'str:50'),
        ('facility', 'str:50'),
        ('school_dis', 'str:50'),
        ('city_acres', 'float:4.2'),
        ('dist_acres', 'float:5.2'),
        ('enhanced', 'str:2'),
        ('strt_date','str:10'),
        ('term_yrs','int:2'),
        ('exp_date','str:10'),
        ('address','str:50'),
        ('community','str:50'),
        ('notes','str:50'),
        ('council_di','int:1'),
        ('serv_dist','int:2'),
        ('playground','int:1'),
        ('tot_lot','int:1'),
        ('playg_inst','int:1'),
        ('baseb_50_6','int:1'),
        ('baseb_90','int:1'),
        ('softball','int:1'),
        ('multi_purp','int:1'),
        ('basketball','int:1'),
        ('tennis_ct','int:1'),
        ('sand_vball','int:1'),
        ('field_ligh','int:1'),
        ('comfort_st','int:1'),
        ('concess_st','int:1')
    ])

gtype = 'Polygon'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)
    #                                  where=" ")

    logging.info('Processing {layername} df.'.format(layername=layername))
    df = df.rename(columns={
      'CD': 'council_di',
      'facillity_': 'facility',
      'enhanced_': 'enhanced',
      'multipurp': 'multi_purp',
      'basketb': 'basketball',
      'tennisct': 'tennis_ct'
      })
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
