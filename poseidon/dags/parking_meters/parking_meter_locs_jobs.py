"""Parking meters locs _jobs file."""
import string
import os
import pandas as pd
import glob
import logging
import numpy as np
from datetime import datetime, timedelta, date
from trident.util import general


conf = general.config
cur_yr = general.get_year()
cur_mon = datetime.now().month
portal_fname = conf['prod_data_dir'] +'/treas_parking_meters_loc_datasd_v1.csv'

def ftp_download_wget():
 """Download parking meters data from FTP."""
 
 wget_str = "wget -np --continue " \
 + "--user=$ftp_user " \
 + "--password='$ftp_pass' " \
 + "--directory-prefix=$temp_dir " \
 + "ftp://ftp.datasd.org/uploads/IPS/SanDiegoInventoryData_{0}{1}*.csv".format(cur_yr, cur_mon)
 tmpl = string.Template(wget_str)
 command = tmpl.substitute(
 ftp_user=conf['ftp_datasd_user'],
 ftp_pass=conf['ftp_datasd_pass'],
 temp_dir=conf['temp_data_dir'])
 
 return command

def build_prod_file(**kwargs):
    """Process parking meters data."""

    # Look for files for the past week
    # Inventory files have two digit days
    # Transaction files only have two digits past 9
    logging.info("Reading in files from previous week")
    logging.info(f"Starting from {datetime.now()}")
    list_ = []

    for i in range(0,7):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        filename = f"SanDiegoInventoryData_{date}.csv"
        logging.info(f"Looking for {filename}")
        files = glob.glob(f"{conf['temp_data_dir']}/{filename}")
        for file_ in files:
            try:
                df = pd.read_csv(file_, dtype={'Pole':str,
                    'Latitude':np.float,
                    'Longitude':np.float
                    })
                df.loc[:,'date_inventory'] = date
                df.loc[:,'Latitude'] = df['Latitude'].apply(lambda x: "{0:.6f}".format(x))
                df.loc[:,'Longitude'] = df['Longitude'].apply(lambda x: "{0:.6f}".format(x))
                list_.append(df)
                logging.info(f"Read {filename}")
            except:
                logging.info(f"{filename} is not available") 

    logging.info(f"Concatting files")
    
    frame = pd.concat(list_, ignore_index=True)
    frame.columns = ['zone','area','sub_area','pole','lat','lng','config_id','config_name','date_inventory']
    frame.loc[:,'date_inventory'] = frame['date_inventory'].apply(
        lambda x: pd.to_datetime(
            x,
            errors='coerce',
            format='%Y%m%d'))

    logging.info(f"Concat has {frame.shape[0]} rows")

    logging.info("Reading in production file")
    prod_file = pd.read_csv(portal_fname,
        parse_dates=['date_inventory'],
        dtype={'lat':str,'lng':str}
        )
    final_temp = pd.concat([prod_file,frame],ignore_index=True)
    final_temp = final_temp.sort_values(by=['pole','date_inventory'])
    logging.info(f"Temp file has {final_temp.shape[0]} rows")
    final = final_temp.drop_duplicates(['pole','lat','lng','config_id'],keep='first')
    final.loc[:,'config_name'] = final['config_name'].apply(lambda x: x.replace('"',''))
    logging.info(f"Final file has {final.shape[0]} rows")


    general.pos_write_csv(
        final,
        portal_fname,
        date_format=conf['date_format_ymd'])  

    return "Updated prod file"
