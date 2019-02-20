"""_jobs file for tsw_integration."""
import os
import pandas as pd
import geopandas as gpd
import logging
from shapely.geometry import Point
from trident.util import general
from trident.util.geospatial import geocode_address_google
from trident.util.geospatial import get_address_for_apn
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
    username = conf['mrm_sf_user']
    password = conf['mrm_sf_pass']
    security_token = conf['mrm_sf_token']

    report_id = "00Ot0000000TPXC"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info('Pull report {} from SF'.format(report_id))

    df = sf.get_report_df(report_id)

    logging.info('Process report {} data.'.format(report_id))

    general.pos_write_csv(df, temp_file_sf, date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully wrote {} records for tsw_sf violations file".format(
        df.shape[0])



def get_pts_violations():
    """Get violations from pts, creates temp file."""
    logging.info('Retrieving PTS violations.')
    db = cx_Oracle.connect(credentials)
    sql = general.file_to_string('./sql/pts_sw.sql', __file__)
    df = pd.read_sql_query(sql, db)

    general.pos_write_csv(df, temp_file_pts, date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully wrote {} records for dsd_pts violations file".format(
        df.shape[0])







def combine_violations():
    """Combine violations from 3 different sources."""

    ptsv = _clean_pts_violations()
    logging.info("Read {} violations from PTS".format(ptsv.shape[0]))
    vpm = _clean_vpm_violations()
    logging.info("Read {} violations from VPM".format(vpm.shape[0]))
    sfv = _clean_sf_violations()
    logging.info("Read {} violations from SFV".format(sfv.shape[0]))

    vs = pd.concat([ptsv, sfv, vpm])
    #vs = pd.concat([ptsv, sfv])

    vs.ADDITIONAL_1 = vs.ADDITIONAL_1.astype(str)
    vs.ADDITIONAL_2 = vs.ADDITIONAL_2.astype(str)
    vs.ADDITIONAL_1 = vs.ADDITIONAL_1.apply(lambda x: ''.join(e for e in x if e.isalnum()))
    vs.ADDITIONAL_2 = vs.ADDITIONAL_2.apply(lambda x: ''.join(e for e in x if e.isalnum()))


    logging.info("There are {} total violations".format(vs.shape[0]))
    logging.info("There are {} non geocoded entries".format(
        vs[((vs.LON.isnull() | vs.LAT.isnull()))].shape[0]))

    # Load current prod file if exists:
    if os.path.isfile(prod_file_csv):
        global georef;
        georef = pd.read_csv(prod_file_csv)
        georef = georef.drop_duplicates(subset='ADDRESS')
        georef = georef[['ADDRESS', 'LON', 'LAT', 'PARCEL_APN']]
        georef = georef[((georef.LON.notnull()) | (georef.LON.notnull()))]


    vs = vs.apply(_get_geocode, axis=1)

    vs['LON'] = pd.to_numeric(vs.LON, downcast='float')
    vs['LAT'] = pd.to_numeric(vs.LAT, downcast='float')

    vs_full = vs.copy()

    # Lock down a separate csv
    vs = vs[((vs.LON.notnull()) | (vs.LAT.notnull()))]
    vs['COORD'] = list(zip(vs.LON, vs.LAT))
    vs['COORD'] = vs.COORD.apply(Point)
    vs = gpd.GeoDataFrame(vs, geometry='COORD')

    logging.info(vs.groupby('SRC').count()["UUID"])

    logging.info("Writing GeoJSON File.")
    if os.path.isfile(prod_file_geojson):
        os.remove(prod_file_geojson)

    vs.to_file(prod_file_geojson, driver="GeoJSON")

    logging.info("Writing CSV File.")
    vs.to_csv(prod_file_csv)
    vs_full.to_csv(prod_file_csv_with_null_geos)

    return "Successfully Merged and Wrote Files."


def _clean_pts_violations():
    """ Clean data coming in from PTS """

    ptsv = pd.read_csv(temp_file_pts)

    ptsv.loc[ptsv.ASSESSOR_PARCEL_10.isnull(), "ASSESSOR_PARCEL_10"] = 0
    ptsv['PARCEL_APN'] = ptsv.ASSESSOR_PARCEL_10.astype(int)
    ptsv['LON'] = ptsv.LONGITUDE.astype(str)
    ptsv['LAT'] = ptsv.LATITUDE.astype(str)
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
    vpm['LON'] = ''
    vpm['LAT'] = ''
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
        'BMP Compliance Deadline': str
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



    sfv.loc[sfv.PARCEL_APN.isnull(), "PARCEL_APN"] = 0
    sfv['PARCEL_APN'] = sfv.PARCEL_APN.astype(int)
    sfv['SRC'] = 'TSW_SF'
    sfv['LON'] = ''
    sfv['LAT'] = ''
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




def _get_geocode(row):
    ## Execute only if lon / lat is empty
    if (row['LON'] == '') or (row['LAT'] == ''):
        address = row['ADDRESS']
        apn = row['PARCEL_APN']
        ## Try grabbing from georef first.
        if (georef is not None):
            logging.info("Using GEOREF")
            if len(georef.loc[georef.PARCEL_APN.astype(str) == apn]) > 0:
                ref_row = georef.loc[georef.PARCEL_APN.astype(str) == apn]
                ref_row = ref_row.loc[0,]
                row['ADDRESS'] = ref_row.ADDRESS
                row['LON'] = ref_row.LON
                row['LAT'] = ref_row.LAT

                return row


            if ((address != '') or (address != 'nan')) and len(georef.loc[georef.ADDRESS.astype(str) == address]) > 0:
                ref_row = georef.loc[georef.ADDRESS.astype(str) == address]
                ref_row.reset_index(drop=True, inplace=True)
                ref_row = ref_row.loc[0,]
                row['ADDRESS'] = ref_row.ADDRESS
                row['LON'] = ref_row.LON
                row['LAT'] = ref_row.LAT

                return row

        if (address == '') or (address == 'nan'):
            address = get_address_for_apn(row['PARCEL_APN'])

        gres = geocode_address_google(address, 'San Diego', 'CA')

        row['LON'] = gres[1]
        row['LAT'] = gres[0]
        row['ADDRESS'] = address
        logging.info("Final LON: {}; Final LAT: {}; Final ADDRESS: {}".format(
            row['LON'],
            row['LAT'],
            row['ADDRESS']))

    return row





