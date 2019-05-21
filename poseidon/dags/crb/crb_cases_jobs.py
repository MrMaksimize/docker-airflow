""" CRB _jobs file """

from trident.util import general
import logging
import subprocess
import os
import pandas as pd
import re

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
        + " cd \"PublicServ-Shared/" \
        + "CitizenReviewBrd/CRB/" \
        + "Case Tracking Information/" \
        + "CRB Case Tracking FY 2018-2019\";" \
        + " get CRB Case Tracking FY 2018-2019.xlsx {temp_path}/crb_cases_fy18-19.xlsx'"

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
    fy_regx = re.compile(r'^[fF][yY][0-9][0-9]$')
    
    temp_cols = ['#',
    'case',
    'team',
    'assigned',
    'completed',
    'presented',
    'days',
    '60_days_or_less', 
    '90days_or_less',
    '120days_or_less',
    'allegation',
    'ia_finding',
    'crb_decision',
    'changes',
    'vote',
    'unanimous_vote',
    'incident_address',
    'pd_division',
    'body_camera?',
    "complainant's_name",
    'race_0',
    'gender_0',
    "officer's_name",
    'race',
    'gender',
    'years_of_service']

    logging.info("Looping through temp files to find crb case tracking excel docs")

    for f in temp_files:
        if 'crb_cases' in f:
            logging.info(f"Found {f} excel, processing")
            file_path = f"{temp_path}/{f}"
            file_read = pd.read_excel(file_path,sheet_name=None,header=None)
            keys = file_read.keys()
            logging.info("Looking in Excel for fy sheets")
            for ky in keys:
                if fy_regx.match(ky):
                    # Reading from row 3
                    fy_crb_rows = file_read[ky].loc[3:].copy()
                    # Creating column list from line 2
                    fy_cols = [str(x).strip().lower().replace(' ','_')
                                for x in file_read[ky].iloc[2]]
                    # Duplicate column names for race and gender
                    comp_race = next(((i, v) for (i, v) in enumerate(fy_cols) if v == 'race'), None)
                    comp_gend = next(((i, v) for (i, v) in enumerate(fy_cols) if v == 'gender'), None)
                    fy_cols[comp_race[0]] = 'race_0'
                    fy_cols[comp_gend[0]] = 'gender_0'
                    fy_crb_rows.columns = fy_cols
                    # Appending sheet data to data list
                    data.append(fy_crb_rows[temp_cols])
                    logging.info(f"Read {ky} sheet from {f}")

    df = pd.concat(data,ignore_index=True)

    logging.info("Renaming columns")

    df = df.rename(columns={'#':'id',
    'case':'case_number',
    'assigned':'date_assigned',
    'completed':'date_completed',
    'presented':'date_presented',
    'days':'days_number',
    '60_days_or_less':'days_60_or_less', 
    '90days_or_less':'days_90_or_less',
    '120days_or_less':'days_120_or_less',
    'body_camera?':'body_camera',
    "complainant's_name":'complainant_name',
    'race_0':'complainant_race',
    'gender_0':'complainant_gender',
    "officer's_name":'officer_name',
    'race':'officer_race',
    'gender':'officer_gender',
    'years_of_service':'officer_yrs_of_svce'})

    # Cannot publish officer name, complainant name,
    # officer race, gender, or yrs of service
    df = df.drop(['complainant_name',
        'officer_name',
        'officer_race',
        'officer_gender',
        'incident_address',
        'officer_yrs_of_svce'
        ],axis=1)

    logging.info("Filling in missing values for rows belonging to same case")

    df = df.fillna(method='ffill')

    prod_file_name = 'crb_cases_datasd'

    general.pos_write_csv(
        df,
        f"{prod_path}/{prod_file_name}.csv")

    return "Successfully processed CRB cases"