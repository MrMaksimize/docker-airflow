"""Parking meters _jobs file."""
import string
import os
import pandas as pd
import glob
import logging
from datetime import datetime, timedelta, date
from trident.util import general
from subprocess import Popen, PIPE, check_output
import subprocess
from shlex import quote
from airflow import AirflowException

conf = general.config
portal_fname = f"{conf['prod_data_dir']}/treas_parking_payments_"

def download_latest(**context):
    """Download parking meters data from FTP."""
    
    file_date = context['execution_date']

    # Need NON zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.month}" \
    f"{file_date.day}"

    fpath = f"SanDiegoData_{filename}_954.csv"

    logging.info(f"Checking FTP for {filename}")

    command = f"cd {conf['temp_data_dir']} && " \
    f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
    f"-o {fpath} " \
    f"ftp://ftp.datasd.org/uploads/IPS/" \
    f"{fpath} -sk"

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return filename

def build_prod_file(**context):
    """Process parking meters data."""

    filename = context['task_instance'].xcom_pull(task_ids='get_parking_files')
    fpath = f"SanDiegoData_{filename}_954.csv"
    
    logging.info(f"Looking for {fpath}")

    
    update = pd.read_csv(f"{conf['temp_data_dir']}/{fpath}")

    logging.info(f"Read in {update.shape[0]} new records")

    # Clean the schema
    logging.info("Cleaning the schema")

    update = update.rename(columns={
        'MeterType': 'meter_type',
        'StartDateTime': 'date_trans_start',
        'ExpiryTime': 'date_meter_expire',
        'Amount': 'trans_amt',
        'TransactionType': 'pay_method',
        'StartDateTime': 'date_trans_start',
        'PoleSerNo': 'pole_id'
    })

    # Convert datetime columns to dt
    logging.info("Setting datetime for update file")
    update['date_trans_start'] = pd.to_datetime(update['date_trans_start'],
                                           format="%m/%d/%Y %H:%M:%S",
                                           errors='coerce')

    update['date_meter_expire'] = pd.to_datetime(update['date_meter_expire'],
                                            format="%m/%d/%Y %H:%M:%S",
                                            errors='coerce')

    # Removing any errant old records from before 2019
    update = update[update['date_trans_start'] >= '01/01/2019 00:00:00']

    # Convert transactions to cents.
    logging.info("Converting update transactions to cents")
    update['trans_amt'] = update['trans_amt'] * 100
    update['trans_amt'] = update['trans_amt'].astype(int)

    # Standardize fields
    logging.info("Standardizing update fields")
    update.meter_type = update.meter_type.str.upper()
    update.pole_id = update.pole_id.str.upper()
    update.pay_method = update.pay_method.str.upper()
    update.meter_type = update.meter_type.str.extract('(SS|MS)', expand=False)
    
    # Rearrange column order
    logging.info("Rearranging column order")
    cols = update.columns.tolist()
    # Set last column as first
    cols = cols[-1:] + cols[:-1]
    update = update[cols]

    logging.info("Sorting by transaction start")
    update = update.sort_values(by='date_trans_start')

    logging.info("Files sometimes contain two years")

    last_yr = int(filename[0:4])-1

    curr_set = update[update['date_trans_start'] >= f"01/01/{filename[0:4]} 00:00:00"]
    last_set = update[update['date_trans_start'] <= f"01/01/{last_yr} 00:00:00"]

    curr_prod = f"{portal_fname}{filename[0:4]}_datasd_v2.csv"
    to_agg = [curr_prod]

    if os.path.isfile(curr_prod):
        logging.info(f"Found {filename[0:4]} file")
        portal = pd.read_csv(curr_prod,low_memory=False)
        logging.info("Concatenating update + portal")
        portal_up = pd.concat([portal, curr_set])

    else:
        logging.info(f"Did not find {filename[0:4]} file")
        logging.info(f"Starting a new file for {filename[0:4]}")
        portal_up = update

    logging.info("Dropping duplicates")
    logging.info(f"Starting with {portal_up.shape[0]}")

    portal_up = portal_up.drop_duplicates()

    logging.info(f"Ending with {portal_up.shape[0]}")
    logging.info(portal_up.head())
    logging.info(f"Writing updated data for {filename[0:4]}")
    general.pos_write_csv(
        portal_up,
        curr_prod,
        date_format=conf['date_format_ymd_hms'])

    if not last_set.empty:

        logging.info("Need to add records to previous year")

        last_prod = f"{portal_fname}{last_yr}_datasd_v2.csv"
        to_agg.append(last_prod)

        portal = pd.read_csv(last_prod,low_memory=False)
        logging.info("Concatenating update + portal")
        portal_up = pd.concat([portal, last_set])

        logging.info("Dropping duplicates")
        logging.info(f"Starting with {portal_up.shape[0]}")

        portal_up = portal_up.drop_duplicates()

        logging.info(f"Ending with {portal_up.shape[0]}")
        logging.info(portal_up.head())
        logging.info(f"Writing updated data for {last_yr}")
        general.pos_write_csv(
            portal_up,
            last_prod,
            date_format=conf['date_format_ymd_hms'])
 
    return to_agg

def build_aggregation(agg_type="pole_by_month", **kwargs):
    """Aggregate raw production data by month/day."""
    out_fname = 'treas_meters_{0}_{1}_datasd_v2.csv'.format(cur_yr,agg_type)

    logging.info("Reading portal data " + portal_fname)
    portal = pd.read_csv(portal_fname)

    logging.info("Translate start_date to dt, create agg columns")

    portal['date_trans_start'] = pd.to_datetime(
                                portal['date_trans_start'],
                                format="%Y-%m-%d %H:%M:%S",
                                errors='coerce')
    portal['month'] = portal.date_trans_start.dt.month
    portal['day'] = portal.date_trans_start.dt.day

    logging.info("Creating " + agg_type + " aggregation")
    if agg_type == 'pole_by_month':
        grouped = portal.groupby(['pole_id', 'month'],
                                 as_index=False)

        aggregation = grouped['trans_amt'].agg({
                                                'sum_trans_amt': 'sum',
                                                'num_trans': 'count'
                                              })
    elif agg_type == 'pole_by_mo_day':
        grouped = portal.groupby(['pole_id', 'month', 'day'],
                                 as_index=False)

        aggregation = grouped['trans_amt'].agg({
                                                'sum_trans_amt': 'sum',
                                                'num_trans': 'count'
                                              })
    else:
        raise NotImplementedError("Not sure what " + agg_type + " is")

    new_file_path = '{0}/{1}'.format(conf['prod_data_dir'],out_fname)

    logging.info("Writing " + agg_type + " aggregation")
    general.pos_write_csv(
        aggregation,
        new_file_path,
        date_format=conf['date_format_ymd_hms'])

    return "Updated agg " + agg_type + " file " + new_file_path
