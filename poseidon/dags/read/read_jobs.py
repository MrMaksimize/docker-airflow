"""READ _jobs file."""
import string
import pandas as pd
import numpy as np
from trident.util import general
import subprocess
from subprocess import Popen, PIPE
from shlex import quote
import logging

conf = general.config

datasets = {
'leases':{
    'ftp':'City\ Property\ Leases.csv',
    'prod':'city_property_leases'
},
'properties': {
    'ftp':'City\ Property\ Details.csv',
    'prod':'city_property_details'
},
'billing': {
    'ftp':'City\ Property\ Billing.csv',
    'prod':'city_property_billing'
},
'parcels': {
    'ftp':'City\ Property\ Parcels.csv',
    'prod':'city_property_parcels'
}
}

def get_file(mode=''):
    """Get READ billing data from FTP."""
    
    mode_files = datasets.get(mode)
    out_file = f"{conf['temp_data_dir']}/{datasets.get(mode_files['prod'])}"
    fpath = f"ToSanDiego/{mode_files['ftp']}"
    
    command = f"curl -o {out_file} " \
            + "sftp://sftp.wizardsoftware.net/"\
            + f"{fpath} " \
            + f"-u {conf['ftp_read_user']}:{conf['ftp_read_pass']} -k"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return f"Successfully downloaded file for {mode}"

def process_billing():
    """Process billing data."""
    df = pd.read_csv(
        f"{conf['temp_data_dir']}/city_property_billing.csv",
        low_memory=False,
        error_bad_lines=False,
        encoding='cp1252')

    df = df.drop('RentCode', 1)

    df = df.rename(columns={
        'LesseeName': 'lessee_name',
        'RecordDate': 'date_billing_record',
        'RecordType': 'line_type_calc',
        'InvoiceNumber': 'invoice_number',
        'PeriodCovered': 'period_covered',
        'Amount': 'AR_line_amt_display',
        'Status': 'line_status_calc',
        'InvoiceDue': 'date_invoice_due'
    })

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/city_property_billing_datasd_v1.csv",
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed billing data.'


def process_leases():
    """Process leases data."""
    df = pd.read_csv(
        f"{conf['temp_data_dir']}/city_property_leases.csv",
        low_memory=False,
        error_bad_lines=False,
        encoding='cp1252')

    df = df.rename(columns={
        'SiteCode': 'site_code',
        'LesseeName': 'lessee_name',
        'LesseeCompany': 'lessee_company',
        'LesseeDBA': 'lessee_DBA',
        'LesseeZip': 'address_zip',
        'LeaseType': 'lease_record_type',
        'Description': 'lease_description',
        'Status': 'lease_status',
        'Location': 'lease_location_name',
        'Nonprofit': 'nonprofit_lessee',
        'EffectiveDate': 'date_effective',
        'SchedTermination': 'date_sched_termination',
        'BillingRentCode': 'rent_code',
        'RentAmount': 'cost_line_amt_USD'
    })

    df['nonprofit_lessee'] = df['nonprofit_lessee'].fillna(0)

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/city_property_leases_datasd_v1.csv",
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed leases data.'


def process_parcels():
    """Process parcels data."""
    df = pd.read_csv(
        f"{conf['temp_data_dir']}/city_property_parcels.csv",
        low_memory=False,
        error_bad_lines=False,
        encoding='cp1252')

    df = df.drop('Comments', 1)

    df = df.rename(columns={'SiteCode': 'site_code', 'ParcelNumber': 'APN-8'})

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/city_property_parcels_datasd_v1.csv",
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed parcels data.'


def process_properties_details():
    """Process properties details data."""
    df = pd.read_csv(
        f"{conf['temp_data_dir']}/city_property_details.csv",
        low_memory=False,
        error_bad_lines=False,
        encoding='cp1252')

    df = df.rename(columns={
        'SiteCode': 'site_code',
        'FileCode': 'file_code',
        'Grantor': 'grantor',
        'MonthAcquired': 'month_acquired',
        'YearAcquired': 'year_acquired',
        'PurchaseFund': 'purchase_fund',
        'LandCost': 'land_cost',
        'BldgCost': 'building_cost',
        'ClosingCost': 'closing_cost',
        'SiteName': 'site_name',
        'ManagingGroup': 'managing_group',
        'ManagingDept': 'managing_dept',
        'DesignatedUse': 'designated_use',
        'SiteAcres': 'site_acres',
        'SubsiteAcres': 'file_acres',
        'SubsiteOriginalAcres': 'original_acres',
        'DedicatedPark': 'dedicated_park',
        'WaterUse': 'water_use',
        'UseRestrictions': 'use_restrictions',
        'ResOrOrd': 'desig_reso_ord',
        'ResOrOrdDate': 'date_reso_ord'
    })

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/city_property_details_datasd_v1.csv",
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed properties details data.'
