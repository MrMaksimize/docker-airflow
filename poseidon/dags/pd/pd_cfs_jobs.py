"""PD calls_for_service _jobs file."""
import string
import os
import logging
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from trident.util import general

conf = general.config
curr_year = datetime.now().strftime('%Y')


def get_cfs_data():
    """Download daily raw cfs data from FTP."""
    wget_str = "wget -np --continue " \
        + "--user=$ftp_user " \
        + "--password='$ftp_pass' " \
        + "--directory-prefix=$temp_dir " \
        + "ftp://ftp.datasd.org/uploads/sdpd/" \
        + "calls_for_service/calls_for_service_*_*_{0}.csv".format(curr_year)

    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
        ftp_user=conf['ftp_datasd_user'],
        ftp_pass=conf['ftp_datasd_pass'],
        temp_dir=conf['temp_data_dir']
    )

    return command


def process_cfs_data():
    """Update production data with new data."""
    logging.info('Combining daily CFS files.')
    path = conf['temp_data_dir']
    allFiles = glob.glob(os.path.join(path, f"calls_for_service_*_*_{curr_year}.csv"))
    np_array_list = []
    for file_ in allFiles:
        df = pd.read_csv(file_, header=None, error_bad_lines=False, low_memory=False)
        np_array_list.append(df.as_matrix())
        
    comb_np_array = np.vstack(np_array_list)

    temp_frame = pd.DataFrame(comb_np_array)

    dtypes = {'address_number_primary':str,
    'beat':str,
    'priority':str,
    'day_of_week':str
    }

    logging.info('Adding recent data to CFS production file.')
    curr_frame = pd.read_csv(
        f"{conf['prod_data_dir']}/pd_calls_for_service_{curr_year}_datasd.csv",
        parse_dates=['date_time'],dtype=dtypes)

    columns_names = curr_frame.columns.values
    temp_frame.columns = columns_names

    temp_frame['date_time'] = pd.to_datetime(temp_frame['date_time'],errors='coerce')

    for str_col in dtypes.keys():
        temp_frame[str_col] = temp_frame[str_col].astype(str)

    prod_frame = curr_frame.append(temp_frame, ignore_index=True)
    prod_frame = prod_frame.drop_duplicates(subset=['incident_num'])
    prod_frame = prod_frame.sort_values(by='date_time', ascending=True)

    logging.info('Exporting updated CFS production data to csv.')

    prod_file = conf['prod_data_dir'] \
        + '/pd_calls_for_service_' \
        + curr_year \
        + '_datasd.csv'

    general.pos_write_csv(
        prod_frame,
        prod_file,
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed CFS data.'
