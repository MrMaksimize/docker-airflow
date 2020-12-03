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
path_xlsx = conf['crb_xls']
cases_fname = 'crb_cases'
alleg_fname = 'crb_allegations'
bwc_fname = 'crb_cases_bwc'


def get_crb_excel():
    """Use mget on to download CRB Excel files."""

    # The exact name of the needed Excel is impossible to predict
    # The name is stored as an environment variable in AWS

    logging.info('Retrieving CRB Excel files.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"PublicServ-Shared/" \
        + "CitizenReviewBrd/CRB/" \
        + "Case Tracking Information/" \
        + "CRB CASE TRACKING/\";" \
        + " lcd \"/data/temp/\";" \
        + " get {path_xlsx}'"

    command = command.format(adname=conf['svc_acct_user'],
                             adpass=conf['svc_acct_pass'],
                             temp_dir=conf['temp_data_dir'])

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

#: Helper function
def get_officer_anon(g):
    
    g['pid'] = g['officer_name'].astype('category').cat.codes
    g['pid'] = g['pid'].apply(lambda x: x+1)
    
    return g

#: DAG function
def create_crb_cases_prod():
    """ Pick up CRB excel from temp and process """

    # Regex pattern to use for finding correct sheet
    fy_regx = re.compile(r'(fy[0-9]+)',flags=re.IGNORECASE)
    
    temp_cols = ['#',
    'case',
    'team',
    'assigned',
    'completed',
    'presented',
    'days',
    '30_days_or_less',
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
    "complainants_name",
    'race_0',
    'gender_0',
    "officers_name",
    'race',
    'gender',
    'years_of_service']

    
    file_path = f"{temp_path}/{path_xlsx}"
    file_read = pd.read_excel(file_path,sheet_name=None)
    keys = file_read.keys()
    
    logging.info("Looking in Excel for fy sheet")
    
    file_fy_match = re.search(fy_regx, path_xlsx)
    file_fy_str = file_fy_match.group(0).lower()
    file_fy_yr = int(file_fy_str[2:])
    
    for ky in keys:
        if ky == f"FY{file_fy_yr}" or ky == f"FY{file_fy_yr-2000}":
            logging.info(f"Using sheet {ky}")
            
            df = file_read[ky]
            df.columns = temp_cols

            cases = df.loc[:,['#',
                'case',
                'team',
                'assigned',
                'completed',
                'presented',
                'days',
                '30_days_or_less',
                '60_days_or_less', 
                '90_days_or_less',
                '120_days_or_less',
                'changes',
                'pd_division',
                'bwc_viewed_by_crb_team',
                'race_0',
                'gender_0']].copy()
            
            allegations = df.loc[:,['#',
                'case',
                "officers_name",
                'allegation',
                'ia_finding',
                'crb_decision',
                'vote',
                'unanimous_vote',
                'bwc_on/off',
                ]].copy()
            
            logging.info(f"Read {ky} sheet from {path_xlsx}")

    allegations['vote'] = allegations['vote'].str.split('-').str.join(' ')

    logging.info("Renaming columns for cases")

    cases = cases.rename(columns={'#':'id',
    'case':'case_number',
    'assigned':'date_assigned',
    'completed':'date_completed',
    'presented':'date_presented',
    'days':'days_number',
    '30_days_or_less':'days_30_or_less',
    '60_days_or_less':'days_60_or_less', 
    '90days_or_less':'days_90_or_less',
    '120days_or_less':'days_120_or_less',
    'bwc_viewed_by_crb_team':'crb_viewed_bwc',
    'race_0':'complainant_race',
    'gender_0':'complainant_gender',
    })

    logging.info(f"Starting with {cases.shape[0]} rows in cases")
    cases = cases.dropna(subset=['crb_viewed_bwc'])
    cases = cases.drop_duplicates(subset='id')

    logging.info(f"Ending with {cases.shape[0]} rows after deduplicating on ID")
    
    general.pos_write_csv(
        cases,
        f"{prod_path}/{cases_fname}_{file_fy_str}_datasd.csv")

    logging.info("Renaming columns for allegations")

    allegations = allegations.rename(columns={'#':'id',
    'case':'case_number',
    "officers_name":'officer_name',
    'bwc_on/off':'bwc_on'
    })

    # Need to add an anonymize officer id
    # Cannot stay consistent, so using simple incrementor
    officers = allegations.copy()

    officers_dedupe = officers.copy().dropna(subset=['bwc_on'])
    officers_dedupe = officers_dedupe.drop_duplicates(['id',
        'officer_name'])

    officers_dedupe['pid'] = -1

    officers_anon = officers_dedupe.groupby(['id']).apply(get_officer_anon)
    officers_final = officers_anon.dropna()

    allegations_anon = pd.merge(allegations,officers_final[['id','case_number','officer_name','pid']],
        how='left',
        right_on=['id','case_number','officer_name'],
        left_on=['id','case_number','officer_name'])


    officers_final = officers_final.drop('officer_name',axis=1)

    bwc_rows = officers_final[['id','pid','case_number','bwc_on']]
    bwc_rows = bwc_rows.sort_values(by=['id','pid'],ascending=[False,True])

    logging.info(f"Have {bwc_rows.shape[0]} rows in bwc data")
    
    final_bwc = bwc_rows.drop_duplicates(['id','pid','case_number'])
    logging.info(f"After dedupe: {final_bwc.shape[0]} rows")

    general.pos_write_csv(
        final_bwc,
        f"{prod_path}/{bwc_fname}_{file_fy_str}_datasd.csv")

    # Cannot publish officer name, complainant name,
    # officer race, gender, or yrs of service
    allegations_anon = allegations_anon.drop(['officer_name','bwc_on'],axis=1)
    allegations_anon = allegations_anon.sort_values(by=['id','pid'],ascending=[False,True])
    

    prod_cols = list(allegations_anon.columns)
    prod_cols = [prod_cols[0]] + [prod_cols[-1]] + prod_cols[1:-1]
    #prod_rows = df_anon[prod_cols].copy()
    
    logging.info(f"Have {allegations_anon.shape[0]} allegation rows")
    
    general.pos_write_csv(
        allegations_anon[prod_cols],
        f"{prod_path}/{alleg_fname}_{file_fy_str}_datasd.csv")

    return "Successfully processed CRB cases"