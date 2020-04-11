"""_jobs file for sidewalk oci."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'SIDEWALK'
prod_dir = conf['prod_data_dir']
layername = 'sidewalks_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('seg_id', 'str'),
        ('geojoin_id','str'),
        ('fun_loc_id','str'),
        ('loc_desc', 'str'),
        ('xstrt1', 'str'),
        ('xstrt2', 'str'),
        ('strt_side', 'str'),
        ('orientn', 'str'),
        ('council', 'int'),
        ('comm_plan', 'int'),
        ('material','str'),
        ('width', 'float')
    ])

gtype = 'LineString'

def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table
                                     #where="OWNERSHIP = 'City of San Diego'"
                                     )

    logging.info('Processing {layername} df.'.format(layername=layername))

    df = df.rename(columns={'sapid':'seg_id',
        'cdcode':'council',
        'cpcode':'comm_plan',
        'legacy_id':'geojoin_id',
        'iamfloc':'fun_loc_id',
        'loc_descr':'loc_desc',
        'orientation':'orientn'
        })

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
