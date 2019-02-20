"""READ _jobs file."""
import os
import ftplib
import pandas as pd
import numpy as np
from trident.util import general

conf = general.config

ftp_files = [
    'City Property Billing.csv', 'City Property Leases.csv',
    'City Property Parcels.csv', 'City Property Details.csv'
]

datasd = [
    'city_property_billing_datasd.csv', 'city_property_leases_datasd.csv',
    'city_property_parcels_datasd.csv', 'city_property_details_datasd.csv'
]


def get_billing():
    """Get READ billing data from FTP."""
    filename = conf['temp_data_dir'] + '/' + datasd[0]
    if os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, 'wb')
    ftp = ftplib.FTP('sdftp.fwsftp.com')
    ftp.login(user=conf['ftp_read_user'], passwd=conf['ftp_read_pass'])
    name = ftp_files[0]
    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved billing data."


def get_leases():
    """Get READ leases data from FTP."""
    filename = conf['temp_data_dir'] + '/' + datasd[1]
    if os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, 'wb')
    ftp = ftplib.FTP('sdftp.fwsftp.com')
    ftp.login(user=conf['ftp_read_user'], passwd=conf['ftp_read_pass'])
    name = ftp_files[1]
    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved leases data."


def get_parcels():
    """Get READ parcels data from FTP."""
    filename = conf['temp_data_dir'] + '/' + datasd[2]
    if os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, 'wb')
    ftp = ftplib.FTP('sdftp.fwsftp.com')
    ftp.login(user=conf['ftp_read_user'], passwd=conf['ftp_read_pass'])
    name = ftp_files[2]
    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved parcels data."


def get_properties_details():
    """Get READ properties details data from FTP."""
    filename = conf['temp_data_dir'] + '/' + datasd[3]
    if os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, 'wb')
    ftp = ftplib.FTP('sdftp.fwsftp.com')
    ftp.login(user=conf['ftp_read_user'], passwd=conf['ftp_read_pass'])
    name = ftp_files[3]
    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved properties details data."


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
        'RecordDate': 'billing_record_date',
        'RecordType': 'line_type_calc',
        'InvoiceNumber': 'invoice_number',
        'PeriodCovered': 'period_covered',
        'Amount': 'AR_line_amt_display',
        'Status': 'line_status_calc',
        'InvoiceDue': 'invoice_due_date'
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
        'LesseeZip': 'lessee_ZIP',
        'LeaseType': 'lease_record_type',
        'Description': 'lease_description',
        'Status': 'lease_status',
        'Location': 'lease_location_name',
        'Nonprofit': 'nonprofit_lessee',
        'EffectiveDate': 'effective_date',
        'SchedTermination': 'sched_termination_date',
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
        'ResOrOrdDate': 'reso_ord_date'
    })

    general.pos_write_csv(
        df,
        conf['prod_data_dir'] + '/' + datasd[3],
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed properties details data.'
