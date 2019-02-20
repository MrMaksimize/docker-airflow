"""_jobs file for 'recreation centers' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'REC_CENTERS'
prod_dir = conf['prod_data_dir']
layername = 'rec_centers_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:4'),
        ('rec_bldg', 'str:50'),
        ('park_name', 'str:50'),
        ('address', 'str:50'),
        ('zip', 'int:5'),
        ('sq_ft', 'int:6'),
        ('year_built', 'int:4'),
        ('serv_dist', 'str:3'),
        ('adult_ctr', 'int:1'),
        ('comfort_st', 'int:1'),
        ('comp_rm', 'int:1'),
        ('dance_rm', 'int:1'),
        ('game_rm', 'int:1'),
        ('gymnasium', 'int:1'),
        ('kiln', 'int:1'),
        ('kiln_rm', 'int:1'),
        ('kitchen', 'int:1'),
        ('multp_rm', 'int:1'),
        ('racqb_ct', 'int:1'),
        ('stage', 'int:1'),
        ('teen_ctr', 'int:1'),
        ('tinytot_rm', 'int:1'),
        ('weight_rm', 'int:1'),
        ('current_cd', 'int:1'),
        ('fy13_cd', 'int:1'),
        ('facility_n', 'int:6'),
        ('thomas_bro', 'str:8'),
        ('community', 'str:30')
    ])

gtype = 'Point'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table)

    logging.info('Processing {layername} df.'.format(layername=layername))

    df = df.fillna('')

    df = df.rename(columns={
        'gymnasuim': 'gymnasium',
        'name': 'community'
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
