"""_jobs file for 'addrapn' layer sde extraction."""
from trident.util import general
from trident.util import geospatial
import pandas as pd
from collections import OrderedDict
import logging

conf = general.config
table = 'ADDRAPN'
prod_dir = conf['prod_data_dir']
layername = 'addrapn_datasd'
layer = f"{prod_dir}/{layername}"

dtypes = OrderedDict([
        ('objectid', 'int:9'),
        ('addrnmbr', 'int:6'),
        ('addrfrac', 'str:5'),
        ('addrpdir', 'str:3'),
        ('addrname', 'str:30'),
        ('addrpostd', 'str:5'),
        ('addrsfx', 'str:6'),
        ('addrunit', 'str:6'),
        ('addrzip', 'int:6'),
        ('add_type', 'str:6'),
        ('roadsegid', 'int:9'),
        ('apn', 'int:12'),
        ('asource', 'str:6'),
        ('plcmt_loc', 'str:6'),
        ('community', 'str:20'),
        ('parcelid', 'int:10'),
        ('usng', 'str:20')
    ])

gtype = 'Point'


def sde_to_shp():
    """SDE table to Shapefile."""
    logging.info(f'Extracting {layername} layer from SDE.')
    df = geospatial.extract_sde_data(table=table,
                                     where="ADDRJUR = 'SD'")

    logging.info(f'Processing {layername} df.')
    df = df.rename(columns={'placement_location': 'plcmt_loc',
                            'address_type': 'add_type'})
    df = df.fillna('')

    logging.info(f'Converting {layername} df to shapefile.')
    geospatial.df2shp(df=df,
                      folder=prod_dir,
                      layername=layername,
                      dtypes=dtypes,
                      gtype=gtype,
                      epsg=2230)
    return f'Successfully converted {layername} to shapefile.'
