"""_jobs file for Get It Done."""
import os
import boto3
import pandas as pd
import logging
import datetime as dt
import numpy as np
from trident.util import general, performSD
from trident.util.sf_client import Salesforce
from trident.util.geospatial import spatial_join_pt

conf = general.config

temp_file_gid = conf['temp_data_dir'] + '/gid_temp.csv'
sname_file_gid = conf['temp_data_dir'] + '/gid_sname.csv'
dates_file_gid = conf['temp_data_dir'] + '/gid_dates.csv'
ref_file_gid = conf['temp_data_dir'] + '/gid_ref.csv'
cd_file_gid = conf['temp_data_dir'] + '/gid_cd.csv'
cp_file_gid = conf['temp_data_dir'] + '/gid_cp.csv'
parks_file_gid = conf['temp_data_dir'] + '/gid_parks.csv'

services_file = conf['temp_data_dir'] + '/gid_final.csv'

prod_file_base = conf['prod_data_dir'] + '/get_it_done_'
prod_file_end = 'requests_datasd_v1.csv'
prod_file_gid = prod_file_base + prod_file_end


cw_gid = 'https://datasd-reference.s3.amazonaws.com/gid/gid_crosswalk.csv'

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

def get_gid_requests():
    """Get requests from sf, creates prod file."""
    username = conf['dpint_sf_user']
    password = conf['dpint_sf_pass']
    security_token = conf['dpint_sf_token']

    report_id = "00Ot0000000TUnb"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info('Pull report {} from SF'.format(report_id))

    sf.get_report_csv(report_id, temp_file_gid)

    logging.info('Process report {} data.'.format(report_id))

    return "Successfully pulled Salesforce report"

def update_service_name():
    """ Take various columns and merge for consistent case types"""
    df = pd.read_csv(temp_file_gid,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )

    logging.info("Read {} records from Salesforce report".format(df.shape[0]))

    df.columns = [x.lower().replace(' ','_').replace('/','_') 
        for x in df.columns]

    df = df.fillna('')

    logging.info('Prepped dataframe, fixing simple case record types')

    # If the Case Record Type needs to be corrected, it is corrected in this block

    df.loc[(df['problem_category'] == 'Abandoned Vehicle'),'case_record_type'] = '72 Hour Report'
    
    df.loc[(df['case_record_type'] == 'Street Division Closed Case') |
       (df['case_record_type'] == 'Street Division') |
       (df['case_record_type'] == 'Storm Water') |
       (df['case_record_type'] == 'Storm Water Closed Case')
       , 'case_record_type'] = 'TSW'

    df.loc[df['case_record_type'] == 'Traffic Engineering Closed Case'
        , 'case_record_type'] = 'Traffic Engineering'

    # Next, we are creating two new fields: case_type_new and case_sub_type_new
    # These two fields join with the crosswalk to get the final service_name field
    # We start by creating subsets according to top-level case record type

    logging.info('Subsetting records to be able to fix more case types')
    
    case_types_correct = df.loc[(df['case_record_type'] == 'ESD Complaint/Report') |
        (df['case_record_type'] == 'Storm Water Code Enforcement') |
        (df['case_record_type'] == 'TSW ROW') |
        (df['case_record_type'] == 'Neighborhood Policing')
        ,:]

    case_types_sap = df.loc[(df['case_record_type'] == 'TSW') |
        (df['case_record_type'] == 'Traffic Engineering')
        ,:]

    case_types_dsd = df.loc[df['case_record_type'] == 'DSD',:]

    case_types_parking = df.loc[df['case_record_type'] == 'Parking',:]

    case_types_72hr = df.loc[df['case_record_type'] == '72 Hour Report',:]

    # If it's easy to assign case type and case sub type, that's done here

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

    # 72 hour parking stuff and sap stuff is done here

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
        sname_file_gid, 
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully mapped case record types and service names"

def update_close_dates():
    """ Fix closed date for consistency with SAP """
    df = pd.read_csv(sname_file_gid,
                     low_memory=False,
                     parse_dates=['date_time_opened','date_time_closed']
                     )

    df['sap_notification_number'] = pd.to_numeric(df['sap_notification_number'],errors='coerce')
    
    orig_cols = df.columns.values.tolist()

    # These will be the exports we got from SAP
    date_files = []
    
    s3 = boto3.resource('s3')
    s3_ref = s3.Bucket('datasd-reference')
    for obj in s3_ref.objects.all():
        if obj.key.startswith('gid/cases_'):
            date_files.append(obj.key)

    # We loop through these files and join back to the df
    # to add a new date column
    # We will later merge all date columns into one

    logging.info("Start looping through fix files")

    for index, file in enumerate(date_files):
        logging.info('Processing {}'.format(file))
        path = 'https://datasd-reference.s3.amazonaws.com/{}'.format(file)
        df_date = pd.read_excel(path)
        
        df_date.columns = [x.lower().replace(' ','_').replace('/','_').replace('"','') 
        for x in df_date.columns]

        # Exact column names are unpredictable
        # We need either SAP notification number or case number
        # Plus the field containing new dates

        closed_cols = [col for col in df_date.columns if 'closed' in col]

        if len(closed_cols) > 0:
            df_date[closed_cols[0]] = pd.to_datetime(df_date[closed_cols[0]],errors='coerce')
            new_dates = df_date[closed_cols[0]]
            df_date.insert(0,'new_date_{}'.format(index),new_dates)
            logging.info("Found column referencing closed date")
        else:
            logging.info("Can't locate date column in spreadsheet")

        notification_cols = [col for col in df_date.columns if 'notification' in col]
        case_cols = [col for col in df_date.columns if 'case' in col]

        if len(notification_cols) > 0:


            logging.info("Joining on SAP notification number")
            df_date[notification_cols[0]] = pd.to_numeric(df_date[notification_cols[0]],
                errors='coerce')
            df = pd.merge(df,
                df_date,
                left_on=['sap_notification_number'],
                right_on=[notification_cols[0]],
                how="left"
                )

        elif len(case_cols) > 0:
            
            if len(case_cols) == 1:
                case_to_merge = case_cols[0]
            else:
                for case in case_cols:
                    if "child" in case:
                        case_to_merge = case
                    else:
                        case_to_merge = case_cols[0]
                        
            logging.info("Joining on Salesforce case number with {}".format(case_to_merge))
            df = pd.merge(df,
                df_date,
                left_on=['case_number'],
                right_on=[case_to_merge],
                how="left"
                )

        else:
            logging.info("Can't determine unique identifier to join on")

    # Convert date columns to datetime, then use the earliest date as final correct date
    logging.info("Getting minimum date for all date columns")
    new_date_cols = [col for col in df.columns if 'new_date' in col]
    df.loc[:,new_date_cols] = df.loc[:,new_date_cols].apply(pd.to_datetime, errors='coerce')
    df['min_new_date'] = df[new_date_cols].min(axis=1)
    
    # Discard additional cols from joins
    logging.info("Adding new date to original column list and subsetting data")
    orig_cols.append('min_new_date')
    df_updated = df[orig_cols]
    
    # Split records into those that need to be updated
    # And those that don't
    bad_records = df_updated[df_updated['min_new_date'].notnull()]
    logging.info("Processed {} records with known date error".format(bad_records.shape[0]))
    good_records = df_updated[df_updated['min_new_date'].isnull()]
    logging.info("{} records remain to be searched".format(good_records.shape[0]))

    # Now check for any children that weren't flagged to be updated
    # By looking for case numbers in parent case number column
    
    logging.info("Searching parent case number col for case numbers with known error")

    child_merge = pd.merge(good_records,
        bad_records[['case_number','min_new_date']],
        left_on=['parent_case_number'],
        right_on=['case_number'],
        how="left"
        )

    good_records_new = child_merge.drop(['min_new_date_x','case_number_y'],axis=1)
    good_records_new = good_records_new.rename(columns={'min_new_date_y':'min_new_date',
        'case_number_x':'case_number'})

    child_search_result = good_records_new[good_records_new['min_new_date'].notnull()].shape[0]
    logging.info("Found {} children cases where parent case has known error".format(child_search_result))
    
    # Export missing children to update in Salesforce
    logging.info("Exporting child cases for gid team")
    general.pos_write_csv(
        good_records_new.loc[good_records_new['min_new_date'].notnull(),
            ['case_number',
            'parent_case_number',
            'min_new_date',
            'date_time_closed']],
        conf['temp_data_dir'] + '/gid_children_found.csv'
        )

    logging.info("Recombining records, updating date time closed and status")
    
    all_records = pd.concat([bad_records,good_records_new])

    all_records = all_records.reset_index(drop=True)

    logging.info("Updating closed date with new date where new date is after date time opened")

    all_records.loc[
        all_records['min_new_date'].notnull() &
        (all_records['min_new_date'] > all_records['date_time_opened'])
        ,'date_time_closed'] = all_records.loc[
        all_records['min_new_date'].notnull() &
        (all_records['min_new_date'] > all_records['date_time_opened'])
        ,'min_new_date']

    logging.info("Update closed date with opened date where new date is before date time opened")

    all_records.loc[
        all_records['min_new_date'].notnull() & 
        (all_records['min_new_date'] <= all_records['date_time_opened'])
        ,'date_time_closed'] = all_records.loc[
        all_records['min_new_date'].notnull() &
        (all_records['min_new_date'] <= all_records['date_time_opened'])
        ,'date_time_opened']

    logging.info("Recalculating case age days")

    all_records.loc[
        all_records['min_new_date'].notnull(),
        'case_age_days'] = all_records.loc[
        all_records['min_new_date'].notnull(),
        ['date_time_closed',
        'date_time_opened']].apply(
            lambda x: (x['date_time_closed'] - x['date_time_opened']).days, 
            axis=1)

    all_records.loc[all_records['min_new_date'].notnull(),
                          'mobile_web_status'] = 'Closed'

    all_records = all_records.drop(['min_new_date'],axis=1)

    all_records.loc[
        :,'date_time_closed'] = all_records.loc[
        :,'date_time_closed'].apply(
            lambda x: x.date())

    logging.info("Export file with fixed close dates")

    general.pos_write_csv(
        all_records, 
        dates_file_gid, 
        date_format='%Y-%m-%dT%H:%M:%S%z')


    return "Successfully updated closed datetime from SAP errors"

def update_referral_col():
    """ Fill in missing referral values """
    df = pd.read_csv(dates_file_gid,
                     low_memory=False,
                     parse_dates=['date_time_opened','date_time_closed']
                     )

    df['referred'] = ''

    df.loc[df['referral_email_list'].notnull(),
        'referred'] = df.loc[df['referral_email_list'].notnull(),
        'referral_email_list']

    df.loc[df['referred_department'].notnull(),
        'referred'] = df.loc[df['referred_department'].notnull(),
        'referred_department']

    df.loc[df['display_referral_information'].notnull(),
        'referred'] = df.loc[df['display_referral_information'].notnull(),
        'display_referral_information']

    general.pos_write_csv(
        df,
        ref_file_gid, 
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully updated referral col" 

def join_council_districts():
    """Spatially joins council districts data to GID data."""
    cd_geojson = conf['prod_data_dir'] + '/council_districts_datasd.geojson'
    gid_cd = spatial_join_pt(ref_file_gid,
                             cd_geojson,
                             lat='lat',
                             lon='long')
    
    cols = gid_cd.columns.values.tolist()
    drop_cols = [
        'objectid',
        'area',
        'perimeter',
        'name',
        'phone',
        'website'
        ]

    if "level_0" in cols:
        drop_cols.append('level_0')


    gid_cd = gid_cd.drop(drop_cols, axis=1)

    gid_cd['district'] = gid_cd['district'].fillna('0')
    gid_cd['district'] = gid_cd['district'].astype(int)
    gid_cd['district'] = gid_cd['district'].astype(str)
    gid_cd['district'] = gid_cd['district'].replace('0', '')

    general.pos_write_csv(
        gid_cd,
        cd_file_gid,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully joined council districts to GID data"

def join_community_plan():
    """Spatially joins community plan districts data to GID data."""
    cp_geojson = conf['prod_data_dir'] + '/cmty_plan_datasd.geojson'
    gid_cp = spatial_join_pt(cd_file_gid,
                             cp_geojson,
                             lat='lat',
                             lon='long')

    cols = gid_cp.columns.values.tolist()
    drop_cols = [
        'objectid',
        'acreage',
        ]

    if "level_0" in cols:
        drop_cols.append('level_0')

    gid_cp = gid_cp.drop(drop_cols, axis=1)

    general.pos_write_csv(
        gid_cp,
        cp_file_gid,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully joined community plan districts to GID data"

def join_parks():
    """Spatially joins community plan districts data to GID data."""
    parks_geojson = conf['prod_data_dir'] + '/parks_datasd.geojson'
    gid_parks = spatial_join_pt(cp_file_gid,
                             parks_geojson,
                             lat='lat',
                             lon='long')

    cols = gid_parks.columns.values.tolist()
    drop_cols = [
        'objectid',
        'gis_acres',
        'location'
    ]

    if "level_0" in cols:
        drop_cols.append('level_0')

    gid_parks = gid_parks.drop(drop_cols, axis=1)

    gid_parks = gid_parks.rename(columns={'name':'park_name'})

    general.pos_write_csv(
        gid_parks,
        parks_file_gid,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully joined parks to GID data"

def create_prod_files():
    """ Map category columns and divide by year """

    df = pd.read_csv(parks_file_gid, 
        low_memory=False,
        parse_dates=['date_time_opened',
        'date_time_closed']
        )

    logging.info("Changing float dtypes to int")
    
    df.loc[:,['sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']] = df.loc[:,['sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']].fillna(-999999.0).astype(int)

    df = df.replace(-999999,'')
    
    df['parent_case_number'] = df['parent_case_number'].replace(0,'')

    df.loc[:,['parent_case_number',
        'sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']] = df.loc[:,['parent_case_number',
        'sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']].astype(str)

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
        'parent_case_number':'service_request_parent_id',
        'case_category':'service_name',
        'date_time_opened':'date_requested',
        'date_time_closed':'date_updated',
        'district':'council_district',
        'cpcode':'comm_plan_code',
        'cpname':'comm_plan_name',
        'name':'park_name',
        'long':'lng'
        })

    final_reports = final_reports[[
    'service_request_id',
    'service_request_parent_id',
    'sap_notification_number',
    'date_requested',
    'case_age_days',
    'service_name',
    'case_record_type',
    'date_updated',
    'status',
    'lat',
    'lng',
    'council_district',
    'comm_plan_code',
    'comm_plan_name',
    'park_name',
    'case_origin',
    'specify_the_issue',
    'referred',
    'public_description'
    ]]

    final_reports = final_reports.sort_values(by=['service_request_id','date_requested','date_updated'])

    general.pos_write_csv(
        final_reports,
        services_file,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    logging.info("Creating new compressed json")
    #json_subset = final_reports.drop(['public_description'],axis=1)
    final_json = final_reports.to_json(f'{conf["prod_data_dir"]}/get_it_done_reports.json',
        orient='records',
        compression='gzip'
        )

    min_report = final_reports['date_requested'].min().year
    max_report = final_reports['date_requested'].max().year

    final_reports = final_reports.drop(['specify_the_issue'],axis=1)

    for year in range(min_report,max_report+1):
        this_yr = str(year)
        next_yr = str(year+1)
        logging.info('Subsetting records for {}'.format(year))
        file_path = prod_file_base+this_yr+'_'+prod_file_end
        file_subset = final_reports[
            (final_reports['date_requested'] >= this_yr+'-01-01 00:00:00') &
            (final_reports['date_requested'] < next_yr+'-01-01 00:00:00')]

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
    fields = ['service_request_id', 'date_requested', 'date_updated',
              'case_origin', 'service_name', 'status', 'lat', 'lng']

    # Filter on field
    gid = gid[fields]

    # Convert datetime columns
    gid['date_requested'] = pd.to_datetime(gid['date_requested'])
    gid['date_updated'] = pd.to_datetime(gid['date_updated'])

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

    range_start_year = range_start.year
    range_start_month = range_start.month
    range_start_day = range_start.day

    range_start_naive = dt.datetime(range_start_year,range_start_month,range_start_day)

    # Get closed potholes, last x days
    potholes_sub = gid_ph_closed.loc[gid_ph_closed['date_requested'] >= range_start_naive]

    potholes_closed = potholes_sub.shape[0]

    # Return expected dict
    return {'value': potholes_closed}
    
def GID_sidewalk_sonar():
    performSD.sonar()
    return "Successfully triggered sonar sidewalks backlog monitoring"
