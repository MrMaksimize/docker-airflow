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
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"PublicServ-Shared/" \
        + "CitizenReviewBrd/CRB/" \
        + "Case Tracking Information/" \
        + "CRB CASE TRACKING/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget *.xlsx'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_officer_anon(g):
    
    g['pid'] = g['officer_name'].astype('category').cat.codes
    g['pid'] = g['pid'].apply(lambda x: x+1)
    
    return g

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
    '90_days_or_less',
    '120_days_or_less',
    'allegation',
    'ia_finding',
    'crb_decision',
    'changes',
    'vote',
    'unanimous_vote',
    'incident_address',
    'pd_division',
    'bwc_viewed_by_crb_team',
    'bwc_on/off',
    "complainant's_name",
    'race_0',
    'gender_0',
    "officer's_name",
    'race',
    'gender',
    'years_of_service']

    logging.info("Looping through temp files to find crb case tracking excel docs")

    for f in temp_files:
        if 'CRB Case Tracking' in f:
            logging.info(f"Found {f} excel, processing")
            file_path = f"{temp_path}/{f}"
            file_read = pd.read_excel(file_path,sheet_name=None,header=None)
            keys = file_read.keys()
            logging.info("Looking in Excel for fy sheets")
            for ky in keys:
                if fy_regx.match(ky):
                    logging.info(f"Using sheet {ky}")
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
                    # Removing blank rows using Allegation columns
                    fy_crb_rows = fy_crb_rows.dropna(subset=['allegation'])
                    fy_crb_rows['#'] = fy_crb_rows['#'].fillna(method='ffill')
                    fy_crb_rows['case'] = fy_crb_rows['case'].fillna(method='ffill')
                    fy_crb_rows["officer's_name"] = fy_crb_rows["officer's_name"].fillna(method='ffill')
                    # Appending sheet data to data list
                    data.append(fy_crb_rows[temp_cols])
                    logging.info(f"Read {ky} sheet from {f}")

    df = pd.concat(data,ignore_index=True)

    df['vote'] = df['vote'].str.split('-').str.join(' ')

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
    'years_of_service':'officer_yrs_of_svce',
    'bwc_on/off':'bwc_on'})

    # Breaking out officers
    officer_series = df['officer_name'].str.split(';').apply(pd.Series, 1).stack()
    officer_series.index = officer_series.index.droplevel(-1)
    officer_series.name = 'officer_name'

    df = df.rename(columns={'officer_name':'officer_name_orig'})
    df = df.join(officer_series)
    df = df.reset_index(drop=True)

    # Need to add an anonymous officer id
    officers = df.loc[:,['id',
    'case_number',
    'officer_name',
    'bwc_on']]

    officers_dedupe = officers.copy().dropna(subset=['bwc_on'])
    officers_dedupe = officers_dedupe.drop_duplicates(['id','officer_name'])

    officers_dedupe['pid'] = -1

    officers_anon = officers_dedupe.groupby(['id']).apply(get_officer_anon)
    officers_final = officers_anon.dropna()

    df_anon = pd.merge(df,officers_final[['id','case_number','officer_name','pid']],
        how='left',
        right_on=['id','case_number','officer_name'],
        left_on=['id','case_number','officer_name'])


    officers_final = officers_final.drop('officer_name',axis=1)

    officers_final = officers_final[['id','pid','case_number','bwc_on']]
    officers_final = officers_final.sort_values(by=['id','pid'],ascending=[False,True])

    general.pos_write_csv(
        officers_final,
        f"{prod_path}/crb_cases_bwc_datasd.csv")

    # Cannot publish officer name, complainant name,
    # officer race, gender, or yrs of service
    df_anon = df_anon.drop(['complainant_name',
        'officer_name',
        'officer_name_orig',
        'officer_race',
        'officer_gender',
        'incident_address',
        'officer_yrs_of_svce',
        'bwc_viewed_by_crb_team'
        ],axis=1)

    logging.info("Filling in missing values for rows belonging to same case")

    df_anon = df_anon.fillna(method='ffill')
    df_anon = df_anon.sort_values(by=['id','pid'],ascending=[False,True])
    prod_cols = list(df_anon.columns)
    prod_cols = [prod_cols[0]] + [prod_cols[-1]] + prod_cols[1:-1]
    prod_file = df_anon[prod_cols]

    prod_file_name = 'crb_cases_datasd'
    prod_file2 = prod_file.drop(['bwc_on'],axis=1)

    general.pos_write_csv(
        prod_file,
        f"{prod_path}/{prod_file_name}.csv")

    general.pos_write_csv(
        prod_file2,
        f"{prod_path}/crb_cases_nobwc_datasd.csv")

    return "Successfully processed CRB cases"