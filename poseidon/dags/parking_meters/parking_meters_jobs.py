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
    """
    Download parking meters data from FTP.
    """
    
    file_date = context['execution_date'].in_tz(tz='US/Pacific')

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

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        return [file_date.year,file_date.month,file_date.day]

def build_prod_file(year=2020,**context):
    """Process parking meters data."""

    date_parts = context['task_instance'].xcom_pull(task_ids='get_parking_files')
    
    filename = f"{date_parts[0]}" \
    f"{date_parts[1]}" \
    f"{date_parts[2]}"
    
    fpath = f"SanDiegoData_{filename}_954.csv"
        
    logging.info(f"Looking for {fpath}")

    
    update = pd.read_csv(f"{conf['temp_data_dir']}/{fpath}",
        parse_dates=['StartDateTime','ExpiryTime']
        )
    
    # Creating subset of just needed columns in case others show up
    update = update[['PoleSerNo',
    'MeterType',
    'StartDateTime',
    'ExpiryTime',
    'Amount',
    'TransactionType'
    ]]

    logging.info(f"Read in {update.shape[0]} new records")

    # Clean the schema
    logging.info("Cleaning the schema")

    update = update.rename(columns={
        'PoleSerNo': 'pole_id',
        'MeterType': 'meter_type',
        'StartDateTime': 'date_trans_start',
        'ExpiryTime': 'date_meter_expire',
        'Amount': 'trans_amt',
        'TransactionType': 'pay_method'
    })

    # Removing any errant old records from before 2019
    update = update[update['date_trans_start'] >= '01/01/2019 00:00:00']

    # Convert transactions to cents.
    logging.info("Converting updated transactions to cents")
    update['trans_amt'] = update['trans_amt'] * 100
    update['trans_amt'] = update['trans_amt'].astype(int)

    # Standardize fields
    logging.info("Standardizing update fields")
    update.meter_type = update.meter_type.str.upper()
    update.pole_id = update.pole_id.str.upper()
    update.pay_method = update.pay_method.str.upper()
    update.meter_type = update.meter_type.str.extract('(SS|MS)', expand=False)

    logging.info("Sorting by transaction start")
    update = update.sort_values(by='date_trans_start')

    #Files sometimes contain transactions from two years
    curr_yr = date_parts[0]
    last_yr = curr_yr - 1

    curr_set = update[update['date_trans_start'] >= f"01/01/{curr_yr} 00:00:00"]
    last_set = update[(update['date_trans_start'] < f"01/01/{curr_yr} 00:00:00") &
                      (update['date_trans_start'] >= f"01/01/{last_yr} 00:00:00")]

    if curr_set.empty:

        logging.info("Transactions not available for current year")

        context['task_instance'].xcom_push(key='agg', value=False)

        if not last_set.empty:

            logging.info("Need to add records to previous year")

            context['task_instance'].xcom_push(key='year', value=True)

            last_prod = f"{portal_fname}{last_yr}_datasd_v2.csv"

            portal = pd.read_csv(last_prod,
                low_memory=False,
                error_bad_lines=False,)
            logging.info("Concatenating update + portal")
            portal_up = pd.concat([portal, last_set])

            general.pos_write_csv(
                portal_up,
                last_prod,
                date_format=conf['date_format_ymd_hms'])

    else:
    
        logging.info(f"Adding {curr_set.shape[0]} records to {curr_yr}")

        context['task_instance'].xcom_push(key='agg', value=True)

        curr_prod = f"{portal_fname}{curr_yr}_datasd_v2.csv"

        logging.info(f"Appending to file for {curr_yr}")

        if os.path.isfile(curr_prod):
            logging.info(f"Found {curr_yr} file")
            portal = pd.read_csv(curr_prod,
                low_memory=False,
                error_bad_lines=False,)
            logging.info(f"Prod file has {portal.shape[0]} records")
            portal_up = pd.concat([portal, curr_set])

        else:
            logging.info(f"Did not find {curr_yr} file")
            logging.info(f"Starting a new file for {curr_yr}")
            portal_up = curr_set

        
        general.pos_write_csv(
            portal_up,
            curr_prod,
            date_format=conf['date_format_ymd_hms'])

        if not last_set.empty:

            logging.info("Need to add records to previous year")

            context['task_instance'].xcom_push(key='year', value=True)

            last_prod = f"{portal_fname}{last_yr}_datasd_v2.csv"

            portal = pd.read_csv(last_prod,
                low_memory=False,
                error_bad_lines=False,
                )
            logging.info("Concatenating update + portal")
            portal_up = pd.concat([portal, last_set])

            general.pos_write_csv(
                portal_up,
                last_prod,
                date_format=conf['date_format_ymd_hms'])

        else:

            context['task_instance'].xcom_push(key='year', value=False)

     
        return "Successfully built prod file"

def check_year(**context):
    """
    Check output from build_prod_file
    To see if need prev yr branch
    """
    trigger = context['task_instance'].xcom_pull(key='year', task_ids='build_prod_file')
    if trigger:
        return "create_prev_agg"
    else:
        return "update_json_date"

def check_agg(**context):
    """
    Check output from build_prod_file
    To see if need curr yr agg branch
    """
    trigger = context['task_instance'].xcom_pull(key='agg', task_ids='build_prod_file')
    if trigger:
        return "create_curr_agg"
    else:
        return "check_for_last_year"

def build_aggregation(agg_type="pole_by_month", agg_year=2020, **context):
    """Aggregate raw production data by month/day."""

    out_fname = f'treas_meters_{agg_year}_{agg_type}_datasd_v2.csv'

    logging.info(f"Reading portal data {agg_year}")
    portal = pd.read_csv(f"{portal_fname}{agg_year}_datasd_v2.csv",
        low_memory=False,
        parse_dates=['date_trans_start']
        )

    logging.info("Translate start_date to dt, create agg columns")

    portal['month'] = portal.date_trans_start.dt.month
    portal['day'] = portal.date_trans_start.dt.day

    logging.info(f"Creating {agg_type} aggregation")
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

    new_file_path = f"{conf['prod_data_dir']}/{out_fname}"

    logging.info(f"Writing {agg_type} aggregation")
    general.pos_write_csv(
        aggregation,
        new_file_path,
        date_format=conf['date_format_ymd_hms'])

    return f"Updated aggregations for {agg_year}"
