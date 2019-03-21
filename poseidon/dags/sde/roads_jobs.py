"""_jobs file for 'roads' sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'ROADS_ALL'
prod_dir = conf['prod_data_dir']
layername = 'roads_datasd'
layer = prod_dir + '/' + layername

dtypes = OrderedDict([
        ('objectid', 'int:7'),
        ('roadsegid', 'int:7'),
        ('roadid', 'int:7'),
        ('fnode', 'int:7'),
        ('tnode', 'int:7'),
        ('rd20full', 'str:30'),
        ('rd30full', 'str:40'),
        ('speed', 'int:2'),
        ('oneway', 'str:1'),
        ('firedriv', 'str:1'),
        ('funclass', 'str:1'),
        ('segclass', 'str:1'),
        ('segstat', 'str:1'),
        ('dedstat', 'str:1'),
        ('subdivid', 'int:6'),
        ('rightway', 'int:3'),
        ('carto', 'str:2'),
        ('obmh', 'str:1'),
        ('postdate', 'str:10'),
        ('addsegdt', 'str:10'),
        ('min_addr', 'int:5'),
        ('max_addr', 'int:5'),
        ('f_level', 'int:2'),
        ('t_level', 'int:2'),
        ('l_block', 'int:5'),
        ('r_block', 'int:5'),
        ('l_tract', 'int:5'),
        ('r_tract', 'int:5'),
        ('l_beat', 'int:5'),
        ('r_beat', 'int:5'),
        ('l_psblock', 'int:6'),
        ('r_psblock', 'int:6'),
        ('wgs_from_x', 'float:12.6'),
        ('wgs_from_y', 'float:12.6'),
        ('wgs_to_x', 'float:12.6'),
        ('wgs_to_y', 'float:12.6'),
        ('nad_from_x', 'float:10.2'),
        ('nad_from_y', 'float:10.2'),
        ('nad_to_x', 'float:10.2'),
        ('nad_to_y', 'float:10.2'),
        ('rd20pred', 'str:5'),
        ('rd20name', 'str:20'),
        ('rd20sfx', 'str:3'),
        ('rd30pred', 'str:5'),
        ('rd30name', 'str:20'),
        ('rd30sfx', 'str:10')
    ])

gtype = 'LineString'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info('Extracting {layername} layer from SDE.'.format(
        layername=layername))
    df = geospatial.extract_sde_data(table=table,
                                     where="LJURISDIC = 'SD'")

    logging.info('Processing {layername} df.'.format(layername=layername))

    df = df.fillna('')

    df = df.rename(columns={
        'abloaddr': 'min_addr',
        'abhiaddr': 'max_addr',
        'frxcoord': 'nad_from_x',
        'frycoord': 'nad_from_y',
        'toxcoord': 'nad_to_x',
        'toycoord': 'nad_to_y'
    })

    df['postdate'] = pd.to_datetime(df['postdate'], errors='coerce')
    df['postdate'] = df['postdate'].dt.strftime('%Y-%m-%d')

    df['addsegdt'] = pd.to_datetime(df['addsegdt'], errors='coerce')
    df['addsegdt'] = df['addsegdt'].dt.strftime('%Y-%m-%d')

    for index, row in df.iterrows():
        nad_from_x = row['nad_from_x']
        nad_from_y = row['nad_from_y']
        wgs_from_x, wgs_from_y = geospatial.pt_proj_conversion(lon=nad_from_x,
                                                               lat=nad_from_y)
        df.set_value(index, 'wgs_from_x', wgs_from_x)
        df.set_value(index, 'wgs_from_y', wgs_from_y)

        nad_to_x = row['nad_to_x']
        nad_to_y = row['nad_to_y']
        wgs_to_x, wgs_to_y = geospatial.pt_proj_conversion(lon=nad_to_x,
                                                           lat=nad_to_y)
        df.set_value(index, 'wgs_to_x', wgs_to_x)
        df.set_value(index, 'wgs_to_y', wgs_to_y)

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
