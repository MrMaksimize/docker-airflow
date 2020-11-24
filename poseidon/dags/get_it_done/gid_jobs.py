"""_jobs file for Get It Done."""
import os
import boto3
import pandas as pd
import geopandas as gpd
import logging
import datetime as dt
import numpy as np
from trident.util import general
from trident.util.sf_client import Salesforce
from trident.util.geospatial import spatial_join_pt
import csv
from trident.util.geospatial import df_to_geodf_pt

conf = general.config

temp_streets_gid = conf['temp_data_dir'] + '/gid_temp_streetdiv.csv'
temp_other_gid = conf['temp_data_dir'] + '/gid_temp_other.csv'
sname_file_gid = conf['temp_data_dir'] + '/gid_sname.csv'
dates_file_gid = conf['temp_data_dir'] + '/gid_dates.csv'
ref_file_gid = conf['temp_data_dir'] + '/gid_ref.csv'

services_file = conf['temp_data_dir'] + '/gid_final.csv'
full_file = conf['prod_data_dir'] + '/get_it_done_requests_datasd.csv'

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

def get_gid_streets():
    """Get requests from sf, creates prod file."""
    username = conf['dpint_sf_user']
    password = conf['dpint_sf_pass']
    security_token = conf['dpint_sf_token']

    report_id = "00Ot0000000ogtBEAQ"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info(f'Pull report {report_id} from SF')

    sf.get_report_csv(report_id, temp_streets_gid)

    logging.info(f'Process report {report_id} data.')

    return "Successfully pulled Salesforce report"

def get_gid_other():
    """Get requests from sf, creates prod file."""
    username = conf['dpint_sf_user']
    password = conf['dpint_sf_pass']
    security_token = conf['dpint_sf_token']

    report_id = "00Ot0000000TUnb"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info(f'Pull report {report_id} from SF')

    sf.get_report_csv(report_id, temp_other_gid)

    logging.info(f'Process report {report_id} data.')

    return "Successfully pulled Salesforce report"

def update_service_name():
    """ Take various columns and merge for consistent case types"""
    df_streets = pd.read_csv(temp_streets_gid,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )

    df_other = pd.read_csv(temp_other_gid,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )

    df = pd.concat([df_streets,df_other],ignore_index=True)

    logging.info(f"Read {df.shape[0]} records from Salesforce report")

    df = df.rename(columns=lambda x: x.lower().replace(' ','_').replace('/','_'))

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
        (df['case_record_type'] == 'Neighborhood Policing') |
        (df['case_record_type'] == 'Special Situation')
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
                       parking_72hr_types],sort=True,ignore_index=True)

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

    df_rename = df_clean.rename(columns={'geolocation_(latitude)':'lat',
        'geolocation_(longitude)':'long',
        'case_type_new':'case_type',
        'case_sub_type_new':'case_sub_type',
        'age_(days)':'case_age_days'})

    logging.info('Writing clean gid file')

    general.pos_write_csv(
        df_rename, 
        sname_file_gid, 
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully mapped case record types and service names"

def update_close_dates():
    """ Fix closed date for consistency with SAP """
    df = pd.read_csv(sname_file_gid,
                     low_memory=False,
                     parse_dates=['date_time_opened','date_time_closed']
                     )
    logging.info(f"Read {df.shape[0]} records")
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
        logging.info(f'Processing {file}')
        path = f'https://datasd-reference.s3.amazonaws.com/{file}'
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
            df_date.insert(0,f'new_date_{index}',new_dates)
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
                        
            logging.info(f"Joining on Salesforce case number with {case_to_merge}")
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
    logging.info(f"Processed {bad_records.shape[0]} records with known date error")
    good_records = df_updated[df_updated['min_new_date'].isnull()]
    logging.info(f"{good_records.shape[0]} records remain to be searched")

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
    logging.info(f"Found {child_search_result} children cases where parent case has known error")
    
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

    logging.info(f"Read {df.shape[0]} records")

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

    # Fix case age days for referred cases

    df.loc[(df.status == 'Referred'), 
        'case_age_days'] = np.nan

    general.pos_write_csv(
        df,
        ref_file_gid, 
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Successfully updated referral col"

def create_stormwater_gis():
    """ Create a subset for GIS mapping of stormwater cases """
    
    df = pd.read_csv(ref_file_gid,low_memory=False)
    logging.info(f"Original file has {df.shape[0]} records")
    
    subset = df.loc[df['case_record_type'] == 'Storm Water Code Enforcement',
    ['case_number',
    'parent_case_number',
    'status',
    'street_address',
    'zipcode',
    'date_time_opened',
    'lat',
    'long',
    'case_record_type']]

    logging.info(f"Subset has {subset.shape[0]} records")

    gdf = df_to_geodf_pt(subset,lat='lat',lon='long')
    gdf = gdf.set_crs(epsg=4326)
    
    gdf.to_file(f"{conf['prod_data_dir']}/discharges_abated.geojson", 
        driver='GeoJSON')

    df_csv = gdf.drop(columns=['geometry'])

    general.pos_write_csv(
        df_csv,
        f"{conf['prod_data_dir']}/discharges_abated.csv", 
        date_format='%Y-%m-%dT%H:%M:%S%z')
    
    return "Successfully created stormwater gis file"
    

def join_requests_polygons(tempfile='',
    geofile='',
    drop_cols=[],
    outfile=''):
    """Spatially joins council districts data to GID data."""
    geojson = f"{conf['prod_data_dir']}/{geofile}_datasd.geojson"
    df = f"{conf['temp_data_dir']}/{tempfile}.csv"
    join = spatial_join_pt(df,
                             geojson,
                             lat='lat',
                             lon='long')
    
    cols = join.columns.values.tolist()

    if "level_0" in cols:
        drop_cols.append('level_0')


    join = join.drop(drop_cols, axis=1)

    general.pos_write_csv(
        join,
        f"{conf['temp_data_dir']}/{outfile}.csv",
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return f"Successfully joined {geofile} to GID data"

def create_prod_files():
    """ Map category columns and divide by year """

    df = pd.read_csv(f"{conf['temp_data_dir']}/gid_parks.csv", 
        low_memory=False,
        parse_dates=['date_time_opened',
        'date_time_closed']
        )

    # Must drop status column here to rename
    # mobile_web_status to status

    df = df.drop(['status'],axis=1)

    logging.info("Changing float dtypes to int")
    
    df.loc[:,['sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']] = df.loc[:,['sap_notification_number',
        'case_age_days',
        'district',
        'cpcode']].fillna(-999999.0).astype(int)

    df = df.replace(-999999,'')

    df['case_age_days'] = df['case_age_days'].replace(-999999,0)
    df['parent_case_number'] = df['parent_case_number'].replace(0,'')

    df.loc[:,['parent_case_number',
        'sap_notification_number',
        'district',
        'cpcode']] = df.loc[:,['parent_case_number',
        'sap_notification_number',
        'district',
        'cpcode']].astype(str)

    logging.info(f'Loaded {df.shape[0]} total prod records')

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
        'date_time_closed':'date_closed',
        'district':'council_district',
        'cpcode':'comm_plan_code',
        'cpname':'comm_plan_name',
        'name':'park_name',
        'long':'lng',
        'iam_functional_location':'iamfloc',
        'functional_location':'floc',
        'mobile_web_status':'status'
        })

    final_reports = final_reports[[
    'service_request_id',
    'service_request_parent_id',
    'sap_notification_number',
    'date_requested',
    'case_age_days',
    'service_name',
    'case_record_type',
    'date_closed',
    'status',
    'lat',
    'lng',
    'street_address',
    'zipcode',
    'council_district',
    'comm_plan_code',
    'comm_plan_name',
    'park_name',
    'case_origin',
    'specify_the_issue',
    'referred',
    'public_description',
    'iamfloc',
    'floc'
    ]]

    final_reports = final_reports.sort_values(by=['service_request_id','date_requested','date_closed'])
    
    logging.info(f"Full dataset contains {final_reports.shape[0]} records")
    
    #Write services file to temp
    general.pos_write_csv(
        final_reports,
        services_file,
        date_format='%Y-%m-%dT%H:%M:%S%z')
    
    # Write full file to prod
    general.pos_write_csv(
        final_reports,
        full_file,
        date_format='%Y-%m-%d')

    logging.info("Creating new compressed csv for Snowflake")
    csv_subset = final_reports.drop(['public_description'],axis=1)
    general.sf_write_csv(csv_subset,'get_it_done')

    min_report = final_reports['date_requested'].min().year
    max_report = final_reports['date_requested'].max().year

    final_reports = final_reports.drop(['specify_the_issue'],axis=1)

    for year in range(min_report,max_report+1):
        this_yr = str(year)
        next_yr = str(year+1)
        logging.info(f'Subsetting records for {year}')
        file_path = prod_file_base+this_yr+'_'+prod_file_end
        file_subset = final_reports[
            (final_reports['date_requested'] >= this_yr+'-01-01 00:00:00') &
            (final_reports['date_requested'] < next_yr+'-01-01 00:00:00')]

        logging.info(f'Writing {file_subset.shape[0]} records to prod')
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
        raise ValueError(f"{service_name} is not a valid service name")

    data = data.reset_index()

    if service_name != 'Illegal Dumping':
        data = data.drop(['index','specify_the_issue'], axis=1)
    else:
        data = data.drop(['index'], axis=1)

    out_path = prod_file_base + machine_service_name + '_' + prod_file_end

    general.pos_write_csv(data, out_path, date_format='%Y-%m-%dT%H:%M:%S%z')

    return f"Successfully wrote {data.shape[0]} records for gid {machine_service_name} prod file"