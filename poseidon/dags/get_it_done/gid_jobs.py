"""_jobs file for Get It Done."""
import os
import pandas as pd
import logging
import datetime as dt
import numpy as np
from trident.util import general
from trident.util.sf_client import Salesforce
from trident.util.geospatial import spatial_join_pt

conf = general.config

temp_file_gid = conf['temp_data_dir'] + '/gid_temp.csv'
clean_file_gid = conf['temp_data_dir'] + '/gid_clean.csv'
cd_file_gid = conf['temp_data_dir'] + '/gid_cd.csv'
services_file = conf['temp_data_dir'] + '/gid_final.csv'
prod_file_base = conf['prod_data_dir'] + '/get_it_done_'
prod_file_end = 'requests_datasd.csv'
prod_file_gid = prod_file_base + prod_file_end
cw_gid = conf['temp_data_dir'] + '/gid_crosswalk.csv'

def sap_case_type(row):
    if row['sap_subject_category'] != '':
        return row['sap_subject_category']
    elif row['sap_problem_category'] != '':
        return row['sap_problem_category']
    elif row['problem_category'] != '':
        return row['problem_category']
    else:
        return row['case_type']

def sap_case_sub_type(row):
    if row['sap_subject_type'] != '':
        return row['sap_subject_type']
    elif row['sap_problem_type'] != '':
        return row['sap_problem_type']
    elif row['problem_category_detail'] != '':
        return row['problem_category_detail']
    else:
        return None


def get_sf_gid_requests():
    """Get requests from sf, creates prod file."""
    username = conf['mrm_sf_user']
    password = conf['mrm_sf_pass']
    security_token = conf['mrm_sf_token']

    report_id = "00Ot0000000TUnb"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info('Pull report {} from SF'.format(report_id))

    sf.get_report_csv(report_id)

    logging.info('Process report {} data.'.format(report_id))

    df = pd.read_csv(temp_file_gid,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False)

    df.columns = [x.lower().replace(' ','_').replace('/','_')
        for x in df.columns]

    df = df.fillna('')

    logging.info('Fixing case record types')

    df.loc[(df['problem_category'] == 'Abandoned Vehicle'),'case_record_type'] = '72 Hour Report'

    df.loc[(df['case_record_type'] == 'Street Division Closed Case') |
       (df['case_record_type'] == 'Street Division') |
       (df['case_record_type'] == 'Storm Water') |
       (df['case_record_type'] == 'Storm Water Closed Case')
       , 'case_record_type'] = 'TSW'

    df.loc[df['case_record_type'] == 'Traffic Engineering Closed Case'
        , 'case_record_type'] = 'Traffic Engineering'

    logging.info('Subsetting records to be able to fix case types')

    case_types_correct = df.loc[(df['case_record_type'] == 'ESD Complaint/Report') |
        (df['case_record_type'] == 'Storm Water Code Enforcement') |
        (df['case_record_type'] == 'TSW ROW')
        ,:]

    case_types_sap = df.loc[(df['case_record_type'] == 'TSW') |
        (df['case_record_type'] == 'Traffic Engineering')
        ,:]

    case_types_dsd = df.loc[df['case_record_type'] == 'DSD',:]

    case_types_parking = df.loc[df['case_record_type'] == 'Parking',:]

    case_types_72hr = df.loc[df['case_record_type'] == '72 Hour Report',:]

    logging.info('Assigning case types for simple cases')

    correct_types = case_types_correct.assign(
        case_type_new=case_types_correct['case_type'],
        case_sub_type_new='')
    dsd_types = case_types_dsd.assign(
        case_type_new=case_types_dsd['violation_name'],
        case_sub_type_new=case_types_dsd['violation_type'])
    parking_types = case_types_parking.assign(
        case_type_new=case_types_parking['parking_violation_type'],
        case_sub_type_new='')
    parking_72hr_types = case_types_72hr.assign(case_type_new='72 Hour Violation',
        case_sub_type_new='')

    logging.info('Correcting case record type for 72 hour violation')

    parking_72hr_types.loc[parking_72hr_types['case_record_type'] == '72 Hour Report', 'case_record_type'] = 'Parking'

    logging.info('Assigning case types for sap cases')

    get_sap_types = case_types_sap.apply(sap_case_type,axis=1)
    get_sap_sub_types = case_types_sap.apply(sap_case_sub_type,axis=1)

    sap_types = case_types_sap.assign(case_type_new=get_sap_types,
                                               case_sub_type_new=get_sap_sub_types)

    logging.info('Concatting all subsets with corrected types')

    df_mapped = pd.concat([correct_types,
                       sap_types,
                       dsd_types,
                       parking_types,
                       parking_72hr_types],ignore_index=True)

    logging.info('Updating illegal dumping case record type')

    df_mapped.loc[df_mapped['problem_category'] == 'Illegal Dumping',
        'case_record_type'] = "ESD Complaint/Report"

    df_mapped.loc[df_mapped['hide_from_web'] == 1,
        'public_description'] = ""

    df_clean = df_mapped.drop(['sap_problem_category',
                                  'sap_problem_type',
                                  'sap_subject_category',
                                  'sap_subject_type',
                                  'case_type',
                                  'parking_violation_type',
                                  'violation_name',
                                  'violation_type',
                                  'problem_category',
                                  'problem_category_detail',
                                  'problem_group',
                                  'hide_from_web'
                                 ],axis=1)

    logging.info('Converting datetime columns')

    df_clean['date_time_opened'] = pd.to_datetime(df_clean['date_time_opened'],errors='coerce')
    df_clean['date_time_closed'] = pd.to_datetime(df_clean['date_time_closed'],errors='coerce')
    df_clean = df_clean.rename(columns={'geolocation_(latitude)':'lat',
        'geolocation_(longitude)':'long',
        'case_type_new':'case_type',
        'case_sub_type_new':'case_sub_type',
        'age_(days)':'case_age_days',
        'mobile_web_status':'status'
    })

    df_clean.loc[df_clean['status'] == 'Referred','case_age_days'] = np.nan

    logging.info('Writing clean gid file')

    general.pos_write_csv(
        df_clean,
        clean_file_gid,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully wrote records for gid clean file"

def join_council_districts():
    """Spatially joins council districts data to GID data."""
    cd_geojson = conf['prod_data_dir'] + '/council_districts_datasd.geojson'
    gid_cd = spatial_join_pt(clean_file_gid,
                             cd_geojson,
                             lat='lat',
                             lon='long')
    gid_cd = gid_cd.drop([
        'objectid',
        'area',
        'perimeter',
        'name',
        'phone',
        'website'
    ], 1)

    gid_cd['district'] = gid_cd['district'].fillna('0')
    gid_cd['district'] = gid_cd['district'].astype(int)
    gid_cd['district'] = gid_cd['district'].astype(str)
    gid_cd['district'] = gid_cd['district'].replace('0', '')

    general.pos_write_csv(
        gid_cd,
        cd_file_gid,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully joined council districts to GID data"

def create_prod_files():
    """ Map category columns and divide by year """

    df = pd.read_csv(cd_file_gid,
        low_memory=False,
        parse_dates=['date_time_opened',
        'date_time_closed'])

    logging.info('Loaded {} total prod records'.format(df.shape[0]))

    logging.info('Loading in the crosswalk for one case category')

    gid_crosswalk = pd.read_csv(cw_gid,low_memory=False)

    gid_crosswalk = gid_crosswalk.fillna('')

    logging.info('Merging temp records with crosswalk')

    final_reports = pd.merge(df,
        gid_crosswalk,
        how='left',
        left_on=['case_record_type','case_type','case_sub_type'],
        right_on=['case_record_type','case_type','case_sub_type'])

    final_reports['case_category'] = final_reports['case_category'].fillna('')

    logging.info('Making category equal to case type for non matches')

    case_cat_fillin = final_reports.loc[(
        (final_reports['case_category'] == '') &
        (final_reports['case_type'] != '')),
        'case_type']

    final_reports.loc[(
        (final_reports['case_category'] == '') &
        (final_reports['case_type'] != '')),
        'case_category'] = case_cat_fillin

    logging.info("Renaming fields to comply with Open 311")

    final_reports = final_reports.rename(columns={
        'case_number':'service_request_id',
        'case_category':'service_name',
        'date_time_opened':'requested_datetime',
        'date_time_closed':'updated_datetime'

        })

    final_reports = final_reports[[
    'service_request_id',
    'requested_datetime',
    'case_age_days',
    'service_name',
    'case_record_type',
    'updated_datetime',
    'status',
    'lat',
    'long',
    'district',
    'case_origin',
    'specify_the_issue',
    'referred_department',
    'public_description'
    ]]

    final_reports = final_reports.sort_values(by=['service_request_id','requested_datetime','updated_datetime'])

    general.pos_write_csv(
        final_reports,
        services_file,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    min_report = final_reports['requested_datetime'].min().year
    max_report = final_reports['requested_datetime'].max().year

    final_reports = final_reports.drop(['specify_the_issue'],axis=1)

    for year in range(min_report,max_report+1):
        this_yr = str(year)
        next_yr = str(year+1)
        logging.info('Subsetting records for {}'.format(year))
        file_path = prod_file_base+this_yr+'_'+prod_file_end
        file_subset = final_reports[
            (final_reports['requested_datetime'] >= this_yr+'-01-01 00:00:00') &
            (final_reports['requested_datetime'] < next_yr+'-01-01 00:00:00')]

        logging.info('Writing {} records to prod'.format(file_subset.shape[0]))
        general.pos_write_csv(
            file_subset,
            file_path,
            date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully created prod files"

def get_requests_service_name(service_name, machine_service_name):
    """Create GID files by service type."""
    gid_csv = services_file
    gid = pd.read_csv(gid_csv, low_memory=False)
    data = gid.loc[gid['service_name'].str.contains(service_name,na=False), :].copy()

    if data.shape[0] == 0:
        raise ValueError("{} is not a valid service name".format(service_name))

    data = data.reset_index()

    if service_name != 'Illegal Dumping':
        data = data.drop(['index','specify_the_issue'], axis=1)
    else:
        data = data.drop(['index'], axis=1)

    out_path = prod_file_base + machine_service_name + '_' + prod_file_end

    general.pos_write_csv(data, out_path, date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully wrote {} records for gid {} prod file".format(
        data.shape[0], machine_service_name)


def prepare_sonar_gid_data():
    """Prepare GID Sonar data."""
    potholes_file = prod_file_base + "pothole_" + prod_file_end

    # Read CSV
    gid = pd.read_csv(potholes_file)

    # Set accepted fields
    fields = ['service_request_id', 'requested_datetime', 'updated_datetime',
              'case_origin', 'service_name', 'status', 'lat', 'long']

    # Filter on field
    gid = gid[fields]

    # Convert datetime columns
    gid['requested_datetime'] = pd.to_datetime(gid['requested_datetime'])
    gid['updated_datetime'] = pd.to_datetime(gid['updated_datetime'])

    return gid

def build_gid_sonar_ph_closed(**kwargs):
    """Todo use filters for dynamic filtering from this data."""
    range_start = kwargs['range_start']
    gid = prepare_sonar_gid_data()

    # Get only closed potholes
    mask = (gid.service_name == 'Pothole') & \
           (gid.status == 'Closed')

    gid_ph_closed = gid[mask]
    gid_ph_closed = gid_ph_closed.copy()

    # Get closed potholes, last x days
    potholes_closed = gid_ph_closed[gid_ph_closed['requested_datetime'] >=
                                    range_start].shape[0]

    # Return expected dict
    return {'value': potholes_closed}
