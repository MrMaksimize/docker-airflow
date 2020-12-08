"""AMCS _jobs file."""
import pandas as pd
import logging
import os
import datetime
import hashlib
import shutil
from shlex import quote

import subprocess
from trident.util import general
from trident.util.sf_client import Salesforce
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

conf = general.config

previous_run_temp_file1 = conf['temp_data_dir'] + '/amcs_previous_run.csv'
temp_file1 = conf['temp_data_dir'] + '/amcs_sites_temp.csv'
temp_file2 = conf['temp_data_dir'] + '/cleaned_amcs_sites.csv'

temp_file3 = conf['temp_data_dir'] + '/grouped_amcs_sites.csv'
final_file = conf['temp_data_dir'] + '/final_amcs_sites.csv'

# If the server IP ever changes, the smbclient command might fail with NT_STATUS_UNSUCCESSFUL errors

def write_to_shared_drive():
    """Write the file to the share location"""
    logging.info('Retrieving data for current FY.')
    conn = BaseHook.get_connection(conn_id="SVC_ACCT")
    server_ip = Variable.get("AMCS_IP_ADDRESS")
    
    command = "smbclient //csdsdcamcsappt/TOWER7 -W ad -mSMB3 " \
        + f"--user={conf['svc_acct_user']}%{conf['svc_acct_pass']} --ip-address={server_ip} " \
        + f"--directory='/Tower7Prod/GetitDone' -c '" \
        + f" put {final_file} sites_export.csv'"

    command = command.format(quote(command))

    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(output)
    else:
        return 'Successfully retrieved Tower data.'


def get_sites():
    """Get requests from sf, creates prod file."""
    sf_conn = BaseHook.get_connection(conn_id="DPINT_SF_AMCS")

    username = sf_conn.login
    password = sf_conn.password
    security_token = sf_conn.extra_dejson
    report_id = sf_conn.schema
    #report_id = "00Ot0000000QrwU"

    # Init salesforce client
    sf = Salesforce(username, password, security_token['token'])

    # Pull dataframe
    logging.info('Pull report {} from SF'.format(report_id))

    sf.get_report_csv(report_id, temp_file1)

    logging.info('Process report {} data.'.format(report_id))

    return "Pulled Salesforce report"


def get_updates_only():

    last_run = 0
    try:
        last_run = os.path.getmtime(previous_run_temp_file1)
    except:
        last_run = 0

    if last_run != 0:
        # write a backup of the previous run
        shutil.copyfile(previous_run_temp_file1, previous_run_temp_file1 + '.' + datetime.date.today().isoformat())
        # delete old backup file
        ten_days_ago = (datetime.date.today() - datetime.timedelta(days=10)).isoformat()
        try:
            os.remove(previous_run_temp_file1 + '.' + ten_days_ago)
        except:
            pass


        get_diff(previous_file=previous_run_temp_file1, current_file=temp_file1, output_file=temp_file2)

        # collect all the rows that belong to any site in the diff
        latest_df = pd.read_csv(temp_file1, encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)
        diff_df = pd.read_csv(temp_file2, encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)
        changed_site_ids = diff_df['Site ID']

        unique_changed_site_ids = changed_site_ids.drop_duplicates()
        changed_sites = pd.merge(latest_df, unique_changed_site_ids, on='Site ID')

        general.pos_write_csv(
                changed_sites,
                temp_file2,
                date_format='%Y-%m-%dT%H:%M:%S%z')

    else:
        # no previous run, send the whole thing
        shutil.copyfile(temp_file1, temp_file2)


    shutil.copyfile(temp_file1, previous_run_temp_file1)

    return "Removed records that have not changed"


def group_site_containers():

    df = pd.read_csv(temp_file2,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )


    col_split = df['Container Type Name'].apply(lambda x: '' if isinstance(x, float) else x.split(' ')[0])

    container_type_df = df.assign(ContainerType=col_split)

    refuse_bins = container_type_df[container_type_df['ContainerType'] == 'Refuse']
    recycle_bins = container_type_df[container_type_df['ContainerType'] == 'Recycle']

    unique_sites = df.drop_duplicates(subset=['Site ID'])

    grouped = refuse_bins.groupby(['Site ID','ContainerType']).size().reset_index()
    unique_sites = pd.merge(unique_sites, grouped, left_on='Site ID', right_on='Site ID', how='left')

    unique_sites = unique_sites.rename(index=str, columns={0: 'RefuseQty'})

    grouped = recycle_bins.groupby(['Site ID','ContainerType']).size().reset_index()
    unique_sites = pd.merge(unique_sites, grouped, left_on='Site ID', right_on='Site ID', how='left')
    unique_sites = unique_sites.rename(index=str, columns={0: 'RecycleQty'})
    unique_sites = unique_sites.drop(['Container Type Name', 'ContainerType_y', 'ContainerType_x'], axis='columns')
    unique_sites = unique_sites.fillna(value={'Recycle Day Of Week': 0})

    print(unique_sites.dtypes)

    general.pos_write_csv(
        unique_sites,
        temp_file3,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Counted containers"


def add_all_columns():

    df = pd.read_csv(temp_file3,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )
    final = pd.DataFrame()


    final['CurrentAccount'] = df['Site ID']
    final['CustomerName'] = 'CURRENT RESIDENT'
    address = df.apply(lambda x: format_address(x), axis = 1)
    final['CustomerAddressline1'] = address
    final['CustomerUnit'] = df['Apt./Suite Number']
    final['CustomerCity'] = 'San Diego'
    final['CustomerState'] = 'CA'
    final['CustomerZip'] = df['Zip']
    final['CustomerPhone'] = ''
    final['SiteAccountNumber'] = df['Site ID']
    final['SiteName'] = 'CURRENT RESIDENT'
    final['SiteAddressline1'] = address
    final['SiteUnit'] = df['Apt./Suite Number']
    final['SiteCity'] = 'San Diego'
    final['SiteState'] = 'CA'
    final['SiteZip'] = df['Zip']
    final['SitePhone'] = ''
    final['SiteRegion'] = df['Neighborhood Name']
    final['ParcelNumber'] = df['Parcel Number']
    final['Lattitude'] = df['Geolocation (Latitude)']
    final['Longitude'] = df['Geolocation (Longitude)']
    final['BillingName'] = 'CURRENT RESIDENT'
    final['BillingAddressline1'] = address
    final['BillingUnit'] = df['Apt./Suite Number']
    final['BillingCity'] = 'San Diego'
    final['BillingState'] = 'CA'
    final['BillingZip'] = df['Zip']
    final['ServiceArea'] = 'AREA'
    final['Class'] = df['Building Type']
    final['Class Description'] = df['Structure Type Sub CD']
    final['CustomerCompanyID'] = 'CITYOFSD'
    final['Account Manager'] = 'HOUSE'
    final['ARAccount'] = ''
    final['CustomerSince'] = ''
    final['Terms'] = ''
    final['BillingCycle'] = ''
    final['AssessSurcharges'] = 'NO'
    final['AssessFranchiseFees'] = 'NO'
    final['AssessFinanceCharges'] = 'NO'
    final['RefuseQty'] = df['RefuseQty']
    final['RefuseADA'] = df['ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['RefuseEquipment'] = ''
    final['RefuseServiceCode'] = 'REFUSE'
    final['RefuseServiceFrequency'] = '1'
    final['RefuseWeekCode'] = 'W'
    final['RefuseRate'] = '0'

    final['RefuseMonday'] = df.apply(lambda x: '' if (x['Refuse Day Of Week'] != 'Mon' and x['Refuse Day Of Week'] != 1 and x['Refuse Day Of Week From GIS'] != 1) else x['Refuse Route'], axis = 1)
    final['RefuseTuesday'] = df.apply(lambda x: '' if (x['Refuse Day Of Week'] != 'Tue' and x['Refuse Day Of Week'] != 2 and x['Refuse Day Of Week From GIS'] != 2) else x['Refuse Route'], axis = 1)
    final['RefuseWednesday'] = df.apply(lambda x: '' if (x['Refuse Day Of Week'] != 'Wed' and x['Refuse Day Of Week'] != 3 and x['Refuse Day Of Week From GIS'] != 3) else x['Refuse Route'], axis = 1)
    final['RefuseThursday'] = df.apply(lambda x: '' if (x['Refuse Day Of Week'] != 'Thu' and x['Refuse Day Of Week'] != 4 and x['Refuse Day Of Week From GIS'] != 4) else x['Refuse Route'], axis = 1)
    final['RefuseFriday'] = df.apply(lambda x: '' if (x['Refuse Day Of Week'] != 'Fri' and x['Refuse Day Of Week'] != 5 and x['Refuse Day Of Week From GIS'] != 5) else x['Refuse Route'], axis = 1)

    final['RefuseSharedIndicator'] = df['Refuse Shared Indicator']

    final['RefuseStartDate'] = '01011999'
    final['RefuseEndDate'] = ''

    final['RecycleQty'] = df['RecycleQty']

    final['RecycleADA'] = df['ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['RecycleEquipment'] = ''
    final['RecycleServiceCode'] = 'RECYCLE'
    final['RecycleServiceFrequency '] = '1'
    final['RecycleWeekCode'] = df['Recycle Week']
    final['RecycleRate'] = '0'

    final['RecycleMonday'] = df.apply(lambda x: '' if (x['Recycle Day Of Week'] != 'Mon' and x['Recycle Day Of Week'] != 1 and x['Recycle Day Of Week From GIS'] != 1) else x['Recycle Route'], axis = 1)
    final['RecycleTuesday'] = df.apply(lambda x: '' if (x['Recycle Day Of Week'] != 'Tue' and x['Recycle Day Of Week'] != 2 and x['Recycle Day Of Week From GIS'] != 2) else x['Recycle Route'], axis = 1)
    final['RecycleWednesday'] = df.apply(lambda x: '' if (x['Recycle Day Of Week'] != 'Wed' and x['Recycle Day Of Week'] != 3 and x['Recycle Day Of Week From GIS'] != 3) else x['Recycle Route'], axis = 1)
    final['RecycleThursday'] = df.apply(lambda x: '' if (x['Recycle Day Of Week'] != 'Thu' and x['Recycle Day Of Week'] != 4 and x['Recycle Day Of Week From GIS'] != 4) else x['Recycle Route'], axis = 1)
    final['RecycleFriday'] = df.apply(lambda x: '' if (x['Recycle Day Of Week'] != 'Fri' and x['Recycle Day Of Week'] != 5 and x['Recycle Day Of Week From GIS'] != 5) else x['Recycle Route'], axis = 1)

    final['RecycleSharedIndicator'] = df['Recycle Shared Indicator']

    final['RecycleStartDate'] = '01011999'
    final['RecycleEndDate'] = ''
    final['RecycleQty'] = df['RecycleQty']

    final['GreenADA'] = df['ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['GreenEquipment'] = ''
    final['GreenServiceCode'] = 'GREENS'
    final['GreenServiceFrequency'] = '1'
    final['GreenWeekCode'] = df['Greens Week']
    final['GreenRate'] = '0'

    final['GreenMonday'] = df.apply(lambda x: '' if (x['Greens Day Of Week'] != 'Mon' and x['Greens Day Of Week'] != 1 and x['Greens Day Of Week From GIS'] != 1) else x['Greens Route'], axis = 1)
    final['GreenTuesday'] = df.apply(lambda x: '' if (x['Greens Day Of Week'] != 'Tue' and x['Greens Day Of Week'] != 2 and x['Greens Day Of Week From GIS'] != 2) else x['Greens Route'], axis = 1)
    final['GreenWednesday'] = df.apply(lambda x: '' if (x['Greens Day Of Week'] != 'Wed' and x['Greens Day Of Week'] != 3 and x['Greens Day Of Week From GIS'] != 3) else x['Greens Route'], axis = 1)
    final['GreenThursday'] = df.apply(lambda x: '' if (x['Greens Day Of Week'] != 'Thu' and x['Greens Day Of Week'] != 4 and x['Greens Day Of Week From GIS'] != 4) else x['Greens Route'], axis = 1)
    final['GreenFriday'] = df.apply(lambda x: '' if (x['Greens Day Of Week'] != 'Fri' and x['Greens Day Of Week'] != 5 and x['Greens Day Of Week From GIS'] != 5) else x['Greens Route'], axis = 1)

    final['GreenSharedIndicator'] = df['Greens Shared Indicator']

    final['GreenStartDate'] = '01011999'
    final['GreenEndDate'] = ''
    final['XCoordinate'] = df['X-Coordinate']
    final['YCoordinate'] = df['Y-Coordinate']

    final = final.round({'ParcelNumber': 0, 'RefuseQty': 0, 'RecycleQty': 0, 'GreenQty': 0})

    general.pos_write_csv(
        final,
        final_file,
        date_format='%Y-%m-%dT%H:%M:%S%z')


    return "Populated all columns for AMCS sites file."


def format_address(row):

    return (str(row['Street Number']) 
        + ('' if pd.isnull(row['Fraction']) else ' ' + str(row['Fraction']))
        + ('' if pd.isnull(row['Street Direction']) else ' ' + str(row['Street Direction']))
        + ('' if pd.isnull(row['Street Name']) else ' ' + row['Street Name'])
        + ('' if pd.isnull(row['Street Suffix']) else ' ' + str(row['Street Suffix']))
        + ('' if pd.isnull(row['Post Direction']) else ' ' + str(row['Post Direction']))
    )


    
def get_diff(previous_file, current_file, output_file):
    """Use diff command to find changes"""

    # put the header in the output file
    command = f"head -n +1 {current_file} > {output_file}"
    command = command.format(quote(command))
    return_code = subprocess.call(command, shell=True)

    if return_code != 0:
        raise Exception('Could not add headers to output file')

    # skip the header and sort the previous file
    previous_file_sorted = f'{previous_file}.sorted'

    command = f"tail -n +2 {previous_file} | sort > {previous_file_sorted}"
    command = command.format(quote(command))
    return_code = subprocess.call(command, shell=True)
    
    if return_code != 0:
        raise Exception(f'Could not sort {previous_file}')

    # skip the header and sort the current file
    current_file_sorted = f'{current_file}.sorted'
    
    command = f"tail -n +2 {current_file} | sort > {current_file_sorted}"
    command = command.format(quote(command))
    return_code = subprocess.call(command, shell=True)
    
    if return_code != 0:
        raise Exception(f'Could sort {current_file}')

    # diff the files and save the changes and new additions in the output file
    command = f"diff {previous_file_sorted} {current_file_sorted}" \
    +" | grep '> ' | awk '{{print substr($0,3)}}'" \
    +f" >> {output_file}"
    command = command.format(quote(command))
    return_code = subprocess.call(command, shell=True)
    
    if return_code != 0:
        raise Exception('Count not write changes to output file')


