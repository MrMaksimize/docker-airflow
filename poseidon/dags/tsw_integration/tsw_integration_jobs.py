"""_jobs file for tsw_integration."""
import os
import pandas as pd
import geopandas as gpd
import logging
import string
import glob
import numpy as np
from shapely.geometry import Point
from trident.util import general
from trident.util import geospatial
from trident.util.sf_client import Salesforce
from airflow.hooks.postgres_hook import PostgresHook
import cx_Oracle


conf = general.config
credentials = general.source['dsd_permits']

temp_file_sf = conf['temp_data_dir'] + '/tsw_violations_sf_temp.csv'
temp_file_pts = conf['temp_data_dir'] + '/tsw_violations_pts_temp.csv'
temp_file_vpm = conf['temp_data_dir'] + '/tsw_violations_vpm_temp.csv'
prod_file_geojson = conf['prod_data_dir'] + '/stormwater_violations_merged.geojson'
prod_file_csv = conf['prod_data_dir'] + '/stormwater_violations_merged.csv'
prod_file_csv_with_null_geos = conf['prod_data_dir'] + '/stormwater_violations_merged_null_geos.csv'

dump_gz_file = "sd_sw_daily_dump.tar.gz"
dump_sql_file = "sd_daily_dump.sql"

## Name of file on FTP that containes the extracted CSV of VPM vios.
dump_csv_file = "tsw_violations_vpm_temp.csv"

# Global placeholder for ref file
georef = None

geocoded_addresses = 'https://datasd-reference.s3.amazonaws.com/sw_viols_address_book.csv'


# VPM RETRIEVAL SUPPORT METHODS

def get_vpm_violations_wget():
    """Temporary placeholder method for generating a wget to retrieve violations"""

    command = """
    rm -rf {} && wget -np --continue \
    --user={} --password={} \
    --directory-prefix={} \
    ftp://ftp.datasd.org/uploads/virtual_pm/{}
    """.format(
            temp_file_vpm,
            conf['ftp_datasd_user'],
            conf['ftp_datasd_pass'],
            conf['temp_data_dir'],
            dump_csv_file)

    return command



#def ftp_download_wget():
#    """Download sql dump."""
#
#    command = """
#    wget -np --continue \
#    --user={} --password={} \
#    --directory-prefix={} \
#    ftp://ftp.datasd.org/uploads/virtual_pm/{}
#    """.format(conf['ftp_datasd_user'], conf['ftp_datasd_pass'], conf['temp_data_dir'], dump_gz_file)
#
#    return command
#
#
#def get_tar_command():
#    """Extract sql dump."""
#    command = "tar -zxvf {}/{}".format(conf['temp_data_dir'], dump_gz_file)
#    return command
#
#
#
#
#def get_vpm_populate_sql():
#    """ Populate SQL for VPM by running the unzipped SQL query. """
#    query = general.file_to_string("{}/{}".format(conf['temp_data_dir'], dump_sql_file))
#    query = query.decode('UTF-8', errors='ignore')
#    query = query.encode('ascii', errors='ignore')
#    return query
#
#
#
### For when we have a db to query, hopefully this is how we query it.
#def get_vpm_violations():
#    """ Gets violations from temporary mysql db """
#    logging.info("Retrieving VPM Vios")
#    pg_hook = PostgresHook(postgres_conn_id='VPM_TEMP')
#    sql = general.file_to_string('./sql/vpm.sql', __file__)
#    df = pg_hook.get_pandas_df(sql)
#    general.pos_write_csv(df, temp_file_vpm, date_format='%Y-%m-%dT%H:%M:%S%z')
#    return "Successfully wrote {} records for vpm violations file".format(
#        df.shape[0])

# END VPM RETRIEVAL SUPPORT METHODS


def get_sf_violations():
    """Get violations from sf, creates temp file."""
    username = conf['dpint_sf_user']
    password = conf['dpint_sf_pass']
    security_token = conf['dpint_sf_token']

    report_id = "00Ot0000000TPXC"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info(f'Pull report {report_id} from SF')

    df = sf.get_report_df(report_id)

    logging.info(f'Process report {report_id} data.')

    general.pos_write_csv(df, temp_file_sf, date_format='%Y-%m-%dT%H:%M:%S%z')

    return f"Successfully wrote {df.shape[0]} records for tsw_sf violations file"

def get_pts_violations():
    """ Get violations from pts, creates temp file. """

    wget_str = "wget -np --continue " \
     + "--user=$ftp_user " \
     + "--password='$ftp_pass' " \
     + "--directory-prefix=$temp_dir " \
     + "ftp://ftp.datasd.org/uploads/dsd/stormwater/*Panda_Extract_STW_*.csv"
    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
    ftp_user=conf['ftp_datasd_user'],
    ftp_pass=conf['ftp_datasd_pass'],
    temp_dir=conf['temp_data_dir'])

    return command

def combine_violations():
    """Combine violations from 3 different sources."""

    ptsv = _clean_pts_violations()
    logging.info(f"Read {ptsv.shape[0]} violations from PTS")
    vpm = _clean_vpm_violations()
    logging.info(f"Read {vpm.shape[0]} violations from VPM")
    sfv = _clean_sf_violations()
    logging.info(f"Read {sfv.shape[0]} violations from SFV")

    vs = pd.concat([ptsv, sfv, vpm])
    #vs = pd.concat([ptsv, sfv])

    vs.ADDITIONAL_1 = vs.ADDITIONAL_1.astype(str)
    vs.ADDITIONAL_2 = vs.ADDITIONAL_2.astype(str)
    vs.ADDITIONAL_1 = vs.ADDITIONAL_1.apply(lambda x: ''.join(e for e in x if e.isalnum()))
    vs.ADDITIONAL_2 = vs.ADDITIONAL_2.apply(lambda x: ''.join(e for e in x if e.isalnum()))

    logging.info(f"There are {vs.shape[0]} total violations")

    vs = vs.fillna(value={'ADDRESS': '',})
    vs = vs.fillna(value={'PARCEL_APN': '',})

    add_book = pd.read_csv(geocoded_addresses,low_memory=False)
    
    logging.info(f"Fixing {vs.loc[(vs.ADDRESS == '') | (vs.ADDRESS == 'nan')].shape[0]} missing addresses")

    parcel_addresses = vs.apply(parcel_to_address,axis=1)
    parcel_add_df = pd.DataFrame(parcel_addresses)
    #parcel_addr_cols = [col for col in parcel_add_df.columns if 'addr' in col or 'stre' in col or 'suf' in col]
    logging.info(parcel_add_df.head())

    #vs['ADDRESS'] = parcel_addresses
    
    logging.info('Create geo id based on address for merging')
    
    vs['geo_id'] = vs['ADDRESS'].apply(lambda x: x.lower().replace(' ',''))
    
    add_merge = pd.merge(vs,
        add_book,
        how='left',
        left_on='geo_id',
        right_on='geo_id',
        indicator=True
        )

    add_merge['LAT'] = add_merge['lat']
    add_merge['LON'] = add_merge['lng']

    logging.info('Separating matches from unmatched')

    add_matched = add_merge[add_merge['_merge'] == 'both']
    add_unmatched = add_merge[add_merge['_merge'] == 'left_only']

    logging.info(f"There are {add_unmatched.shape[0]} non geocoded entries")

    if add_unmatched.empty:
        logging.info('Nothing to geocode')
        geocoded_all = add_matched.copy()
    else:
        geocode_dedupe = add_unmatched.copy()
        geocode_dedupe = geocode_dedupe.drop_duplicates(subset=['geo_id'])
        geocoder_results = geocode_dedupe.apply(get_violation_coords, axis=1)
        coords = geocoder_results.apply(pd.Series)
        geocode_dedupe['LAT'] = coords[0]
        geocode_dedupe['LON'] = coords[1]

        add_unmatched = add_unmatched.drop(['LAT','LON'],axis=1)
        
        geocoded = pd.merge(add_unmatched,
                geocode_dedupe[['geo_id','LAT','LON']],
                how='left',
                left_on='geo_id',
                right_on='geo_id'
                )
        logging.info("Concat addresses matched with address book and addresses geocoded")
        geocoded_all = pd.concat([add_matched,geocoded],ignore_index=True,sort=False)
        logging.info("Adding new geocodes to address book")
        add_book_adds = geocode_dedupe[['geo_id','LON','LAT']]
        add_book_adds = add_book_adds.rename(columns={'LAT':'lat','LON':'lng'})
        add_book_new = pd.concat([add_book,add_book_adds])
        add_book_new = add_book_new.drop_duplicates(subset=['geo_id'])
        add_book_new.to_csv(conf['prod_data_dir']+'/sw_viols_address_book.csv',index=False)
        
    geocoded_all = geocoded_all.drop(['_merge','geo_id','lat','lng'],axis=1)

    vs_full = geocoded_all.copy()

    # Lock down a separate csv
    vs_geo = geocoded_all[((geocoded_all.LON.notnull()) | (geocoded_all.LAT.notnull()))]
    vs_geo['COORD'] = list(zip(vs_geo.LON, vs_geo.LAT))
    vs_geo['COORD'] = vs_geo.COORD.apply(Point)
    vs_geo = gpd.GeoDataFrame(vs_geo, geometry='COORD')

    logging.info("Writing GeoJSON File.")
    if os.path.isfile(prod_file_geojson):
        os.remove(prod_file_geojson)

    vs_geo.to_file(prod_file_geojson, driver="GeoJSON")

    logging.info("Writing CSV File.")
    vs_geo.to_csv(prod_file_csv)
    vs_full.to_csv(prod_file_csv_with_null_geos)

    return "Successfully Merged and Wrote Files."


def _clean_pts_violations():
    """ Clean data coming in from PTS """

    filename = conf['temp_data_dir'] + "/*Panda_Extract_STW_*.csv"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    ptsv = pd.read_csv(latest_file,names=['INSP_ID',
        'ASSESSOR_PARCEL_10',
        'LATITUDE',
        'LONGITUDE',
        'STREET_ADDRESS',
        'INSP_TYPE_ID',
        'INSP_TYPE_NM',
        'INSP_RESULT_ID',
        'INSP_RESULT_NM',
        'PERFORMED_END_DT',
        'PROJ_TITLE',
        'SCOPE',
        'LOCATION_NOTE',
        'CONSTRUCTION_NOTE'
        ],dtype={'LONGITUDE':np.float64,
        'LATITUDE':np.float64
        })

    ptsv['PARCEL_APN'] = ptsv.ASSESSOR_PARCEL_10
    ptsv['LON'] = ptsv.LONGITUDE
    ptsv['LAT'] = ptsv.LATITUDE
    ptsv['SRC'] = 'DSD_PTS'
    ptsv['TYPE'] = ptsv.INSP_TYPE_NM
    ptsv['STATUS'] = ptsv.INSP_RESULT_NM
    ptsv['UUID'] = (ptsv['SRC'] + '_' + ptsv['INSP_ID'].astype(str).str.replace('-', '_')).str.lower()
    ptsv['ADDRESS'] = ptsv['STREET_ADDRESS'].astype(str)
    ptsv['ISSUE_DATE'] = ptsv['PERFORMED_END_DT']
    ptsv['VIOLATOR'] = ptsv['PROJ_TITLE']
    ptsv['ADDITIONAL_1'] = ptsv['SCOPE']
    ptsv['ADDITIONAL_2'] = ptsv['CONSTRUCTION_NOTE']
    ptsv['COMPLY_BY'] = ''


    ptsv = ptsv[['UUID',
               'SRC',
               'TYPE',
               'STATUS',
               'ISSUE_DATE',
               'COMPLY_BY',
               'PARCEL_APN',
               'LON',
               'LAT',
               'ADDRESS',
               'VIOLATOR',
               'ADDITIONAL_1',
               'ADDITIONAL_2']]

    return ptsv

def _clean_vpm_violations():
    """ Clean vpm violations """

    vpm = pd.read_csv(temp_file_vpm)


    vpm['SRC'] = 'PW_VPM'
    vpm['sw_bmp_report_id'] = vpm['sw_bmp_report_id'].astype(str)
    vpm['project_id'] = vpm['project_id'].astype(str)
    vpm['permit_number'] = vpm.permit_number.astype(str)
    vpm['UUID'] = (vpm['SRC'] + '_' + vpm['sw_bmp_report_id'] + '_' + vpm['project_id']).str.lower()
    vpm['LON'] = np.nan
    vpm['LAT'] = np.nan
    vpm['STATUS'] = vpm['bmpr_state']
    vpm['TYPE'] = vpm.title
    vpm['PARCEL_APN'] = ''
    vpm['ISSUE_DATE'] = vpm['report_date']
    vpm['COMPLY_BY'] = vpm['reinspection_date']
    vpm['ADDRESS'] = vpm.location_street.astype(str)
    vpm['CITY'] = vpm.location_city
    vpm['STATE'] = vpm.location_state
    vpm['ZIP'] = vpm.location_zip
    vpm['VIOLATOR'] = vpm.project_name
    vpm['ADDITIONAL_1'] = vpm.comments
    vpm['ADDITIONAL_2'] = ''


    vpm = vpm[['UUID',
        'SRC',
        'TYPE',
        'STATUS',
        'ISSUE_DATE',
        'COMPLY_BY',
        'PARCEL_APN',
        'LON',
        'LAT',
        'ADDRESS',
        'VIOLATOR',
        'ADDITIONAL_1',
        'ADDITIONAL_2']]

    return vpm



def _clean_sf_violations():
    """ Clean TSW violations from SF """

    sfv = pd.read_csv(temp_file_sf, dtype={
        'Site: Street Number': str,
        'Violation Date Formatted': str,
        'BMP Compliance Deadline': str,
        'Site: Primary Parcel Number': str
    })

    sfv.columns = ['V_NUM',
      'STATUS',
      'STATUS_1',
      'V_TYPE',
      'TYPE',
      'V_DATE',
      'BMP_COMP_DEADLINE',
      'SITE_ID',
      'PARCEL_APN',
      'ADDRESS_NUM',
      'ADDRESS_STREET',
      'ADDRESS_CITY',
      'ADDRESS_STATE',
      'VIOLATOR']

    sfv['SRC'] = 'TSW_SF'
    sfv['LON'] = np.nan
    sfv['LAT'] = np.nan
    sfv['STATUS'] = sfv['STATUS']
    sfv['TYPE'] = sfv['V_TYPE'] + ': ' + sfv['TYPE']
    sfv['UUID'] = (sfv['SRC'] + '_' + sfv['V_NUM'].str.replace('-', '_')).str.lower()
    sfv['ADDRESS'] = (sfv['ADDRESS_NUM'] + ' ' + sfv['ADDRESS_STREET']).astype(str)
    sfv['ISSUE_DATE'] = sfv.V_DATE
    sfv['COMPLY_BY'] = sfv.BMP_COMP_DEADLINE
    sfv['ADDITIONAL_1'] = sfv['STATUS_1']
    sfv['ADDITIONAL_2'] = ''

    sfv = sfv[['UUID',
               'SRC',
               'TYPE',
               'STATUS',
               'ISSUE_DATE',
               'COMPLY_BY',
               'PARCEL_APN',
               'LON',
               'LAT',
               'ADDRESS',
               'VIOLATOR',
               'ADDITIONAL_1',
               'ADDITIONAL_2']]

    return sfv

def parcel_to_address(x):

    if x['ADDRESS'] == '' or x['ADDRESS'] == 'nan':

        if x['PARCEL_APN'] != '':

            return geospatial.get_address_for_apn(x['PARCEL_APN'])

        else:

            return ''

    else:

        return x['ADDRESS']

def get_violation_coords(x):

    if x['ADDRESS'] != '':

        if not x['ADDRESS'].startswith('APN:'):

            return geospatial.census_address_geocoder(address_line=x['ADDRESS'])

        else:

            logging.info("Could not get address from APN")

            return np.nan, np.nan

    else:

        logging.info("Could not find any address information")

        return np.nan, np.nan