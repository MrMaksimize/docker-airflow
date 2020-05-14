"""PD hate crimes _jobs file."""
import glob
import os
import csv
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
        + "Hate_Crimes/Hate_Crimes_Data_Portal_SDPD*.xlsx"

    tmpl = string.Template(wget_str)
    command = tmpl.substitute(
        ftp_user=conf['ftp_datasd_user'],
        ftp_pass=conf['ftp_datasd_pass'],
        temp_dir=conf['temp_data_dir']
    )

    return command


def process_data():
    """Process hate crimes data."""
    
    filename = conf['temp_data_dir'] + "/Hate_Crimes_Data_Portal_SDPD*.xlsx"
    list_of_files = glob.glob(filename)
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

    df = pd.read_excel(latest_file,sheet_name='hate_crimes_datasd')

    df['date'] = pd.to_datetime(df['date'],errors='coerce')
    df['date'] = df['date'].dt.date

    cols = df.columns.to_list()

    # Move datetime column from position 19 to position 5

    new_cols = cols[0:5] + cols[19:20] + cols[5:19] + cols[20:]

    logging.info(len(cols) == len(new_cols))

    final_df = df[new_cols]

    final_df.to_csv(prod_file,
        encoding='utf-8',
        index=False,
        doublequote=True,
        quoting=csv.QUOTE_ALL
        )
    
    return 'Successfully processed hate crimes data.'
