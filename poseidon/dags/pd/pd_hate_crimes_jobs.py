"""PD hate crimes _jobs file."""
import ftplib
import operator
import string
import logging
import pandas as pd
from datetime import datetime
from trident.util import general

conf = general.config
prod_file = f"{conf['prod_data_dir']}/hate_crimes_datasd.csv"


def get_data():
    """Download Hate Crimes data from FTP."""
    wget_str = "wget -np --continue " \
        + "--user=$ftp_user " \
        + "--password='$ftp_pass' " \
        + "--directory-prefix=$temp_dir " \
        + "ftp://ftp.datasd.org/uploads/sdpd/" \
        + "Hate_Crimes/Hate_Crimes_Data_Portal_SDPD_*.xlsx"

    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
        ftp_user=conf['ftp_datasd_user'],
        ftp_pass=conf['ftp_datasd_pass'],
        temp_dir=conf['temp_data_dir']
    )

    return command


def process_data():
    """Process hate crimes data."""
    
    filename = conf['temp_data_dir'] + "/Hate_Crimes_Data_Portal_SDPD_*.xlsx"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    df = pd.read_excel(latest_file,sheet='hate_crimes_datasd')

    final_df = df[['case_number',
                         'date',
                         'year',
                         'month',
                         'time',
                         'date_time',
                         'crime_code',
                         'crime',
                         'beat',
                         'command',
                         'weapon',
                         'motivation',
                         'number_of_suspects',
                         'suspect',
                         'victim_count',
                         'victim_other',
                         'injury',
                         'suspect_race_0',
                         'suspect_race_1',
                         'suspect_race_2',
                         'suspect_sex_0',
                         'suspect_sex_1',
                         'suspect_sex_2',
                         'victim_race_0',
                         'victim_race_1',
                         'victim_race_2',
                         'victim_sex_0',
                         'victim_sex_1',
                         'victim_sex_2'
                        ]]

    general.pos_write_csv(
        final_df,
        prod_file,
        date_format='%Y-%m-%d')
    
    return 'Successfully processed hate crimes data.'
