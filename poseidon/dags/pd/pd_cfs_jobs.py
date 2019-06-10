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
    allFiles = glob.glob(os.path.join(path, "calls*.csv"))
    np_array_list = []
    for file_ in allFiles:
        df = pd.read_csv(file_, header=None, error_bad_lines=False, low_memory=False)
        np_array_list.append(df.as_matrix())
    comb_np_array = np.vstack(np_array_list)

    temp_frame = pd.DataFrame(comb_np_array)

    logging.info('Adding recent data to CFS production file.')
    curr_frame = pd.read_csv(
        conf['prod_data_dir'] +
        '/pd_calls_for_service_' + curr_year + '_datasd_v1.csv')

    columns_names = curr_frame.columns.values
    temp_frame.columns = columns_names
    prod_frame = curr_frame.append(temp_frame, ignore_index=True)
    prod_frame = prod_frame.drop_duplicates(subset=['incident_num'])
    prod_frame['date_time'] = pd.to_datetime(prod_frame['date_time'])
    prod_frame['day'] = prod_frame['day'].astype(int)
    prod_frame['stno'] = prod_frame['stno'].astype(int)
    prod_frame['beat'] = pd.to_numeric(prod_frame['beat'], errors='coerce')
    prod_frame['priority'] = pd.to_numeric(
        prod_frame['priority'], errors='coerce')
    prod_frame = prod_frame.sort_values(by='date_time', ascending=True)

    logging.info('Exporting updated CFS production data to csv.')
    
    prod_frame = prod_frame.rename(columns={'stno':'address_number_primary',
        'stdir1':'address_pd_primary',
        'street':'address_road_primary',
        'streettype':'address_sfx_primary',
        'stdir2':'address_pd_intersecting',
        'stname2':'address_road_intersecting',
        'sttype2':'address_sfx_intersecting'
        })

    prod_file = conf['prod_data_dir'] \
        + '/pd_calls_for_service_' \
        + curr_year \
        + '_datasd_v1.csv'

    general.pos_write_csv(
        prod_frame,
        prod_file,
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed CFS data.'
