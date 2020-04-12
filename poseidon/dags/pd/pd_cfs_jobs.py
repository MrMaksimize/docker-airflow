"""PD calls_for_service _jobs file."""
import logging
import os
import pandas as pd
import pendulum
import subprocess
import glob
from subprocess import Popen, PIPE
from trident.util import general
from shlex import quote

conf = general.config

def get_cfs_data(**context):
    """Download daily raw cfs data from FTP."""

    exec_date = context['execution_date']
    # Exec date returns a Pendulum object
    # Running this job at 5p should capture files for day of exec

    # File name does not have zero-padded numbers
    # But month is spelled, abbreviated
    # Pattern is month_day_year
    filename = f"{exec_date.strftime('%b')}_" \
    f"{exec_date.day}_" \
    f"{exec_date.year}"

    fpath = f"calls_for_service_{filename}.csv"

    logging.info(f"Checking FTP for {filename}")

    command = f"cd {conf['temp_data_dir']} && " \
    f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
    f"-o {fpath} " \
    f"ftp://ftp.datasd.org/uploads/sdpd/calls_for_service/" \
    f"{fpath} -sk"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        # return filename as context for next task
        return filename


def process_cfs_data(**context):
    """Update production data with new data."""

    logging.info("Reading new cfs file")
    filename = context['task_instance'].xcom_pull(task_ids='get_cfs_data')
    fpath = f"calls_for_service_{filename}.csv"

    logging.info(f"Looking for {fpath}")

    temp_frame = pd.read_csv(f"{conf['temp_data_dir']}/{fpath}",
        header=None,
        error_bad_lines=False,
        low_memory=False)

    dtypes = {'address_number_primary':str,
    'call_type':str,
    'beat':str,
    'priority':str,
    'day_of_week':str
    }

    logging.info('Adding recent data to CFS production file.')
    curr_frame = pd.read_csv(
        f"{conf['prod_data_dir']}/pd_calls_for_service.csv",
        parse_dates=['date_time'],
        dtype=dtypes,
        low_memory=False
        )

    col_names = curr_frame.columns.values
    temp_frame.columns = col_names

    logging.info('Fixing dtypes')

    temp_frame.loc[:,'date_time'] = pd.to_datetime(temp_frame.loc[:,'date_time'],
        errors='coerce')

    for str_col in dtypes.keys():
        temp_frame.loc[:,str_col] = temp_frame.loc[:,str_col].astype(str)

    logging.info('Appending new records to existing file')

    prod_frame = curr_frame.append(temp_frame, ignore_index=True)
    logging.info(f'New file has {prod_frame.shape[0]} records')
    prod_frame = prod_frame.drop_duplicates(subset=['incident_num'])
    logging.info(f'After dedupe, file has {prod_frame.shape[0]} records')
    prod_frame = prod_frame.sort_values(by='date_time', ascending=True)

    logging.info('Exporting full data to csv')

    prod_file = f"{conf['prod_data_dir']}/pd_calls_for_service.csv"
    
    general.pos_write_csv(
        prod_frame,
        prod_file,
        date_format=conf['date_format_ymd_hms'])

    logging.info('Splitting off CY files')

    min_yr = temp_frame['date_time'].min().year
    max_yr = temp_frame['date_time'].max().year

    if min_yr != max_yr:

        for i in range(min_yr,max_yr+1):
            
            logging.info(f'Writing new file for {i}')
            
            yr_subset = prod_frame[(prod_frame['date_time'] >= f'01-01-{i} 00:00:00') &
                (prod_frame['date_time'] < f'01-01-{i+1} 00:00:00')]

            yr_file = f"{conf['prod_data_dir']}/pd_calls_for_service_{i}_datasd.csv"

            general.pos_write_csv(
                yr_subset,
                yr_file,
                date_format=conf['date_format_ymd_hms'])

    else:

        yr_file = f"{conf['prod_data_dir']}/pd_calls_for_service_{max_yr}_datasd.csv"

        yr_subset = prod_frame[(prod_frame['date_time'] >= f'01-01-{max_yr} 00:00:00')]

        general.pos_write_csv(
            yr_subset,
            yr_file,
            date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed CFS data.'
