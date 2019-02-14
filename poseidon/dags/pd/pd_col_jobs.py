"""PD collisions _jobs file."""
import ftplib
import operator
import string
import logging
import pandas as pd
from datetime import datetime
from trident.util import general


conf = general.config


def get_collisions_data():
    """Download most recent collisions data from FTP."""
    # find most recent file
    filename = conf['temp_data_dir']+'/temp_collisions.csv'
    file = open(filename, 'wb')
    ftp = ftplib.FTP('ftp.datasd.org')

    ftp.login(user=conf['ftp_datasd_user'], passwd=conf['ftp_datasd_pass'])
    ftp.cwd('uploads/sdpd/collisions')
    ls = []
    ftp.dir('-t', ls.append)
    name = ls[0].split()[8]

    ftp.retrbinary('RETR %s' % name, file.write)
    ftp.close

    return "Successfully retrieved collision data."


def process_collisions_data():
    """Process collision data."""
    prod_file = conf['prod_data_dir']+'/pd_collisions_datasd.csv'
    df = pd.read_csv(conf['temp_data_dir']+'/temp_collisions.csv',
                     header=None,
                     error_bad_lines=False)

    df.columns = [
        'report_id', 'date_time', 'police_beat', 'street_no', 'street_dir',
        'street_name', 'street_type', 'cross_st_dir', 'cross_st_name',
        'cross_st_type', 'violation_section', 'violation_type',
        'charge_desc', 'injured', 'killed', 'hit_run_lvl'
    ]

    general.pos_write_csv(
        df,
        prod_file,
        date_format=conf['date_format_ymd_hms']
    )

    return 'Successfully processed collisions data.'
