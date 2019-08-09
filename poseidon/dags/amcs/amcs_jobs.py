"""AMCS _jobs file."""
import pandas as pd
import logging
from subprocess import Popen, PIPE
from trident.util import general
from trident.util.sf_client import Salesforce

conf = general.config
fy = general.get_FY_year()

temp_file1 = conf['temp_data_dir'] + '/amcs_sites_temp.csv'
temp_file2 = conf['temp_data_dir'] + '/cleaned_amcs_sites.csv'
temp_file3 = conf['temp_data_dir'] + '/final_amcs_sites.csv'

def write_to_shared_drive():
    """Write the file to the share location"""
    logging.info('Retrieving data for current FY.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'cd \"TSW-TEO-Shared/TEO/" \
        + "TEO-Transportation-Systems-and-Safety-Programs/" \
        + "Traffic Data/{fy}/RECORD FINDER\";" \
        + " ls; get Machine_Count_Index.xlsx {temp_dir}/{out_f}.xlsx;'"

    command = command.format(adname=conf['mrm_sannet_user'],
                             adpass=conf['mrm_sannet_pass'],
                             fy=fy,
                             temp_dir=conf['temp_data_dir'],
                             out_f=out_fname)

    logging.info(command)

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(output)
    else:
        return 'Successfully retrieved {} data.'.format(fy)

def get_sites():
    """Get requests from sf, creates prod file."""
    username = conf['mrm_sf_user']
    password = conf['mrm_sf_pass']
    security_token = conf['mrm_sf_token']

    report_id = "00Ot0000000KBxu"

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull dataframe
    logging.info('Pull report {} from SF'.format(report_id))

    sf.get_report_csv(report_id, temp_file1)

    logging.info('Process report {} data.'.format(report_id))

    return "Pulled Salesforce report"


def group_site_containers():

    df = pd.read_csv(temp_file1,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )


    col_split = df['Container Type Name'].apply(lambda x: x.split(' ')[0])

    container_type_df = df.assign(ContainerType=col_split)

    refuse_bins = container_type_df[container_type_df['ContainerType'] == 'Refuse']
    recycle_bins = container_type_df[container_type_df['ContainerType'] == 'Recycle']

    unique_sites = df.drop_duplicates(subset=['Site: Site ID'])
    unique_sites.head()

    grouped = refuse_bins.groupby(['Site: Site ID','ContainerType']).size().reset_index()
    unique_sites = pd.merge(unique_sites, grouped, left_on='Site: Site ID', right_on='Site: Site ID', how='left')

    unique_sites = unique_sites.rename(index=str, columns={0: 'RefuseQty'})

    unique_sites.head()
    #pd.merge(df, refuse_df, on='Site: Site ID')

    grouped = recycle_bins.groupby(['Site: Site ID','ContainerType']).size().reset_index()
    unique_sites = pd.merge(unique_sites, grouped, left_on='Site: Site ID', right_on='Site: Site ID', how='left')
    unique_sites = unique_sites.rename(index=str, columns={0: 'RecycleQty'})
    unique_sites = unique_sites.drop(['Container Type Name', 'ContainerType_y', 'ContainerType_x'], axis='columns')

    general.pos_write_csv(
        unique_sites,
        temp_file2,
        date_format='%Y-%m-%dT%H:%M:%S%z')

    return "Counted containers"

def add_all_columns():

    df = pd.read_csv(temp_file2,
                     encoding='ISO-8859-1',
                     low_memory=False,
                     error_bad_lines=False,
                     )
    final = pd.DataFrame()


    final['CurrentAccount'] = df['Site: Site ID']
    final['CustomerName'] = 'CURRENT RESIDENT'
    address = df.apply(lambda x: format_address(x), axis = 1)
    final['CustomerAddressline1'] = address
    final['CustomerUnit'] = df['Site: Apt./Suite Number']
    final['CustomerCity'] = 'San Diego'
    final['CustomerState'] = 'CA'
    final['CustomerZip'] = df['Site: Zip']
    final['CustomerPhone'] = ''
    final['SiteAccountNumber'] = df['Site: Site ID']
    final['SiteName'] = 'CURRENT RESIDENT'
    final['SiteAddressline1'] = address
    final['SiteUnit'] = df['Site: Apt./Suite Number']
    final['SiteCity'] = 'San Diego'
    final['SiteState'] = 'CA'
    final['SiteZip'] = df['Site: Zip']
    final['SitePhone'] = ''
    final['SiteRegion'] = df['Site: Neighborhood Name']
    final['ParcelNumber'] = df['Site: Parcel Number']
    final['Lattitude'] = df['Site: Geolocation (Latitude)']
    final['Longitude'] = df['Site: Geolocation (Longitude)']
    final['BillingName'] = 'CURRENT RESIDENT'
    final['BillingAddressline1'] = address
    final['BillingUnit'] = df['Site: Apt./Suite Number']
    final['BillingCity'] = 'San Diego'
    final['BillingState'] = 'CA'
    final['BillingZip'] = df['Site: Zip']
    final['ServiceArea'] = 'AREA'
    final['Class'] = df['Site: Building Type']
    final['Class Description'] = df['Site: Structure Type Sub CD']
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
    final['RefuseADA'] = df['Site: ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['RefuseEquipment'] = ''
    final['RefuseServiceCode'] = 'REFUSE'
    final['RefuseServiceFrequency'] = '1'
    final['RefuseWeekCode'] = 'W'
    final['RefuseRate'] = '0'

    final['RefuseMonday'] = df.apply(lambda x: '' if x['Site: Refuse Day Of Week'] != 'Mon' else x['Site: Refuse Route'], axis = 1)
    final['RefuseTuesday'] = df.apply(lambda x: '' if x['Site: Refuse Day Of Week'] != 'Tue' else x['Site: Refuse Route'], axis = 1)
    final['RefuseWednesday'] = df.apply(lambda x: '' if x['Site: Refuse Day Of Week'] != 'Wed' else x['Site: Refuse Route'], axis = 1)
    final['RefuseThursday'] = df.apply(lambda x: '' if x['Site: Refuse Day Of Week'] != 'Thu' else x['Site: Refuse Route'], axis = 1)
    final['RefuseFriday'] = df.apply(lambda x: '' if x['Site: Refuse Day Of Week'] != 'Fri' else x['Site: Refuse Route'], axis = 1)

    final['RefuseSharedIndicator'] = df['Site: Refuse Shared Indicator']

    final['RefuseStartDate'] = '01011999'
    final['RefuseEndDate'] = ''

    final['RecycleQty'] = df['RecycleQty']

    final['RecycleADA'] = df['Site: ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['RecycleEquipment'] = ''
    final['RecycleServiceCode'] = 'RECYCLE'
    final['RecycleServiceFrequency '] = '1'
    final['RecycleWeekCode'] = df['Site: Recycle Week']
    final['RecycleRate'] = '0'

    final['RecycleMonday'] = df.apply(lambda x: '' if x['Site: Recycle Day Of Week'] != 'Mon' else x['Site: Recycle Route'], axis = 1)
    final['RecycleTuesday'] = df.apply(lambda x: '' if x['Site: Recycle Day Of Week'] != 'Tue' else x['Site: Recycle Route'], axis = 1)
    final['RecycleWednesday'] = df.apply(lambda x: '' if x['Site: Recycle Day Of Week'] != 'Wed' else x['Site: Recycle Route'], axis = 1)
    final['RecycleThursday'] = df.apply(lambda x: '' if x['Site: Recycle Day Of Week'] != 'Thu' else x['Site: Recycle Route'], axis = 1)
    final['RecycleFriday'] = df.apply(lambda x: '' if x['Site: Recycle Day Of Week'] != 'Fri' else x['Site: Recycle Route'], axis = 1)

    final['RecycleSharedIndicator'] = df['Site: Recycle Shared Indicator']

    final['RecycleStartDate'] = '01011999'
    final['RecycleEndDate'] = ''
    final['RecycleQty'] = df['RecycleQty']

    final['GreenADA'] = df['Site: ADA'].apply(lambda x: 'YES' if x == 1 else 'NO')
    final['GreenEquipment'] = ''
    final['GreenServiceCode'] = 'GREENS'
    final['GreenServiceFrequency'] = '1'
    final['GreenWeekCode'] = df['Site: Greens Week']
    final['GreenRate'] = '0'

    final['GreenMonday'] = df.apply(lambda x: '' if x['Site: Greens Day Of Week'] != 'Mon' else x['Site: Greens Route'], axis = 1)
    final['GreenTuesday'] = df.apply(lambda x: '' if x['Site: Greens Day Of Week'] != 'Tue' else x['Site: Greens Route'], axis = 1)
    final['GreenWednesday'] = df.apply(lambda x: '' if x['Site: Greens Day Of Week'] != 'Wed' else x['Site: Greens Route'], axis = 1)
    final['GreenThursday'] = df.apply(lambda x: '' if x['Site: Greens Day Of Week'] != 'Thu' else x['Site: Greens Route'], axis = 1)
    final['GreenFriday'] = df.apply(lambda x: '' if x['Site: Greens Day Of Week'] != 'Fri' else x['Site: Greens Route'], axis = 1)

    final['GreenSharedIndicator'] = df['Site: Greens Shared Indicator']

    final['GreenStartDate'] = '01011999'
    final['GreenEndDate'] = ''
    final['XCoordinate'] = df['Site: X-Coordinate']
    final['YCoordinate'] = df['Site: Y-Coordinate']

    final = final.round({'ParcelNumber': 0, 'RefuseQty': 0, 'RecycleQty': 0, 'GreenQty': 0})

    logging.info('-- Final output --')
    logging.info(final.head())

    general.pos_write_csv(
        final,
        temp_file3,
        date_format='%Y-%m-%dT%H:%M:%S%z')


    return "Populated all columns for AMCS sites file."

def get_updates_only():

    df = pd.read_csv(temp_file2,
            encoding='ISO-8859-1',
            low_memory=False,
            error_bad_lines=False,
            )

    # if previous file exists
    #  load previous file and diff with current
    #  overwrite previous file with current
    #  return df of diff
    # else
    #  overwrite previous file with current
    #  return file


    #merged = df1.merge(df2, indicator=True, how='outer')
    #merged[merged['_merge'] == 'right_only']

    return "Removed records that haven't changed"

def format_address(row):

    return (str(row['Site: Street Number']) 
        + ('' if pd.isnull(row['Site: Fraction']) else ' ' + str(row['Site: Fraction']))
        + ('' if pd.isnull(row['Site: Street Direction']) else ' ' + str(row['Site: Street Direction']))
        + ('' if pd.isnull(row['Site: Street Name']) else ' ' + row['Site: Street Name'])
        + ('' if pd.isnull(row['Site: Street Suffix']) else ' ' + str(row['Site: Street Suffix']))
        + ('' if pd.isnull(row['Site: Post Direction']) else ' ' + str(row['Site: Post Direction']))
    )



