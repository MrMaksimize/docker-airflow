"""READ _jobs file."""
import os
import ftplib
import string
import pandas as pd
import numpy as np
from trident.util import general

conf = general.config

ftp_files = [
    'City\ Property\ Billing.csv', 'City\ Property\ Leases.csv',
    'City\ Property\ Parcels.csv', 'City\ Property\ Details.csv'
]

datasd = [
    'city_property_billing_datasd.csv', 'city_property_leases_datasd.csv',
    'city_property_parcels_datasd.csv', 'city_property_details_datasd.csv'
]


def get_billing():
    """Get READ billing data from FTP."""
    curl_str = "curl -o $out_file " \
            + "sftp://sftp.wizardsoftware.net/"\
            + "$fpath " \
            + "-u $user:$passwd -k"

    tmpl = string.Template(curl_str)
    command = tmpl.substitute(
        out_file=conf['temp_data_dir'] + '/' + datasd[0],
        fpath='ToSanDiego'+ '/' +ftp_files[0],
        user=conf['ftp_read_user'],
        passwd=conf['ftp_read_pass'])

    return command

def get_leases():
    """Get READ leases data from FTP."""
    curl_str = "curl -o $out_file " \
            + "sftp://sftp.wizardsoftware.net/"\
            + "$fpath " \
            + "-u $user:$passwd -k"

    tmpl = string.Template(curl_str)
    command = tmpl.substitute(
        out_file=conf['temp_data_dir'] + '/' + datasd[1],
        fpath='ToSanDiego'+ '/' +ftp_files[1],
        user=conf['ftp_read_user'],
        passwd=conf['ftp_read_pass'])

    return command

def get_parcels():
    """Get READ parcels data from FTP."""
    curl_str = "curl -o $out_file " \
            + "sftp://sftp.wizardsoftware.net/"\
            + "$fpath " \
            + "-u $user:$passwd -k"

    tmpl = string.Template(curl_str)
    command = tmpl.substitute(
        out_file=conf['temp_data_dir'] + '/' + datasd[2],
        fpath='ToSanDiego'+ '/' +ftp_files[2],
        user=conf['ftp_read_user'],
        passwd=conf['ftp_read_pass'])

    return command

def get_properties_details():
    """Get READ parcels data from FTP."""
    curl_str = "curl -o $out_file " \
            + "sftp://sftp.wizardsoftware.net/"\
            + "$fpath " \
            + "-u $user:$passwd -k"

    tmpl = string.Template(curl_str)
    command = tmpl.substitute(
        out_file=conf['temp_data_dir'] + '/' + datasd[3],
        fpath='ToSanDiego'+ '/' +ftp_files[3],
        user=conf['ftp_read_user'],
        passwd=conf['ftp_read_pass'])

    return command

def process_billing():
    """Process billing data."""
    df = pd.read_csv(
        conf['temp_data_dir'] + '/' + datasd[0],
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
        conf['prod_data_dir'] + '/' + datasd[0],
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed billing data.'


def process_leases():
    """Process leases data."""
    df = pd.read_csv(
        conf['temp_data_dir'] + '/' + datasd[1],
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

    general.pos_write_csv(
        df,
        conf['prod_data_dir'] + '/' + datasd[1],
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed leases data.'


def process_parcels():
    """Process parcels data."""
    df = pd.read_csv(
        conf['temp_data_dir'] + '/' + datasd[2],
        low_memory=False,
        error_bad_lines=False,
        encoding='cp1252')

    df = df.drop('Comments', 1)

    df = df.rename(columns={'SiteCode': 'site_code', 'ParcelNumber': 'APN-8'})

    general.pos_write_csv(
        df,
        conf['prod_data_dir'] + '/' + datasd[2],
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed parcels data.'


def process_properties_details():
    """Process properties details data."""
    df = pd.read_csv(
        conf['temp_data_dir'] + '/' + datasd[3],
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
        conf['prod_data_dir'] + '/' + datasd[3],
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed properties details data.'
