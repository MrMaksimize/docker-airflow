""" CRB _jobs file """

from trident.util import general
import logging
import subprocess
import pandas as pd

conf = general.config
prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

def get_crb_excel():
    """Use mget on to download CRB Excel files."""
    logging.info('Retrieving CRB Excel files.')
    adname = conf['mrm_sannet_user']
    adpass = conf['mrm_sannet_pass']
    command = f"smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"PublicServ-Shared/" \
        + "CitizenReviewBrd/CRB/" \
        + "Case Tracking Information/" \
        + "CRB Case Tracking FY 2018-2019\";" \
        + " lcd \"/data/temp/\";" \
        + " mget *.xlsx'"

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def create_crb_cases_prod():
    """ Pick up CRB excel from temp and process """

    temp_files = [f for f in os.listdir(temp_path)]
    data = []

    for f in temp_files:
        if 'CRB Case Tracking' in f:
            file_path = f"{temp_path}/{f}"
            file_read = pd.read_excel(file_path)
            data.append(file_read)
            logging.info(f"Read {f} excel")

    df = pd.concat(data,ignore_index=True)
    df.columns = [x.lower().replace(' ','_') 
        for x in df.columns]

    prod_file_name = 'crb_cases_datasd'

    general.pos_write_csv(
        df,
        f"{prod_path}/{prod_file_name}.csv")

    return "Successfully processed CRB cases"