"""Parking meters locs _jobs file."""
from subprocess import Popen, PIPE
import subprocess
import pandas as pd
import logging
import numpy as np
import pendulum
from trident.util import general
from shlex import quote

conf = general.config
portal_fname = conf['prod_data_dir'] +'/treas_parking_meters_loc_datasd_v1.csv'

def ftp_download(**context):
    """Download parking meters data from FTP."""

    file_date = context['execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    logging.info(context)

    # Need zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.strftime('%m')}" \
    f"{file_date.strftime('%d')}"

    fpath = f"SanDiegoInventoryData_{filename}.csv"

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
        return filename
 
def build_prod_file(**context):
    """Process parking meters data."""

    logging.info("Reading new inventory file")
    filename = context['task_instance'].xcom_pull(task_ids='get_meter_loc_files')
    fpath = f"SanDiegoInventoryData_{filename}.csv"
    
    logging.info(f"Looking for {fpath}")

    df = pd.read_csv(f"{conf['temp_data_dir']}/{fpath}",
        dtype={'Pole':str,
        'Latitude':np.float,
        'Longitude':np.float
        })

    logging.info("Found, and successfully read file")
    logging.info("Setting date inventory and formatting lat/lng")

    df.loc[:,'date_inventory'] = filename
    df.loc[:,'Latitude'] = df['Latitude'].apply(lambda x: "{0:.6f}".format(x))
    df.loc[:,'Longitude'] = df['Longitude'].apply(lambda x: "{0:.6f}".format(x))
    df.columns = ['zone','area','sub_area','pole','lat','lng','config_id','config_name','date_inventory']
    
    logging.info("Converting date inventory col")
    df.loc[:,'date_inventory'] = df.loc[:,'date_inventory'].apply(
        lambda x: pd.to_datetime(
            x,
            errors='coerce',
            format='%Y%m%d'))

    logging.info("Reading in production file")
    
    prod_file = pd.read_csv(portal_fname,
        parse_dates=['date_inventory'],
        dtype={'lat':str,'lng':str}
        )
    logging.info(f"Prod file has {prod_file.shape[0]} rows")
    logging.info(f"New file has {df.shape[0]} rows")

    logging.info("Concat new inventory rows with existing")
    final_temp = pd.concat([prod_file,df],ignore_index=True,sort=False)

    logging.info(f"Concat has {final_temp.shape[0]} rows")
    
    logging.info("Sort by pole ID and descending date of inventory")
    final_temp = final_temp.sort_values(by=['pole','date_inventory'],
        ascending=[True,False])

    logging.info("Separate latitude and longitude")
    temp_ll = final_temp[['pole','lat','lng']]

    logging.info("Keep only the most recent lat and lng")
    temp_ll_dedupe = temp_ll.drop_duplicates(['pole'],keep='first')

    logging.info("Dedupe full dataset on Pole and Config, keeping oldest")
    final_temp_dedupe = final_temp[['zone',
        'area',
        'sub_area',
        'pole',
        'config_id',
        'config_name',
        'date_inventory']].drop_duplicates(['pole','config_id'],keep='last')

    logging.info("Merge deduped datasets")
    final = pd.merge(final_temp_dedupe,
        temp_ll_dedupe,
        how='left',
        on='pole'
        )

    logging.info('Reading in crosswalk')

    crosswalk = pd.read_csv('https://datasd-reference.s3.amazonaws.com/crosswalk_meters.csv')

    logging.info("Merging crosswalk for SAP ID")

    final_sapid = pd.merge(final,crosswalk[['sub_area','sapid']],how='inner',on='sub_area',indicator=True)

    need_segments = final_sapid[final_sapid['_merge'] == 'left_only']

    if need_segments.empty:
        
        logging.info('All sub areas have SAP IDs')

    else:

        logging.info(f"Need to get segments for {need_segments.shape[0]} rows")


    final_sapid = final_sapid.drop(columns='_merge')

    logging.info(f"Final has {final_sapid.shape} shape")

    general.pos_write_csv(
        final_sapid,
        portal_fname,
        date_format=conf['date_format_ymd'])

    return filename

def clean_files(**context):
    """ Delete files that were processed """

    filename = context['task_instance'].xcom_pull(task_ids='build_prod_file')
    
    logging.info("Deleting files from FTP")

    fpath = f"SanDiegoInventoryData_{filename}.csv"

    # Available ftp commands
    """ ABOR ACCT ALLO APPE CDUP CWD  DELE EPRT EPSV FEAT HELP LIST MDTM MKD
    MODE NLST NOOP OPTS PASS PASV PORT PWD  QUIT REIN REST RETR RMD  RNFR
    RNTO SITE SIZE SMNT STAT STOR STOU STRU SYST TYPE USER XCUP XCWD XMKD
    XPWD XRMD """

    ftp_command = f'curl --user {conf["ftp_datasd_user"]}:{conf["ftp_datasd_pass"]} ' \
    f'ftp://ftp.datasd.org/uploads/IPS ' \
    f'-Q "DELE {fpath}"'

    ftp_command = ftp_command.format(quote(ftp_command))

    try:
        p = subprocess.check_output(ftp_command, shell=True, stderr=subprocess.STDOUT)
        logging.info('Deleted file from ftp')
    except subprocess.CalledProcessError as e:
        logging.info("Did not delete file from ftp")
        logging.info(e.output)

    logging.info("Deleting files from temp")

    temp_command = f"cd {conf['temp_data_dir']} && " \
        f"rm {fpath}"

    temp_command = temp_command.format(quote(temp_command))

    try:
        p = subprocess.check_output(temp_command, shell=True, stderr=subprocess.STDOUT)
        logging.info("Deleted file from temp folder")
    except subprocess.CalledProcessError as e:
        logging.info("Did not delete file from temp")
        logging.info(e.output)


    return "Attempted to delete files"
