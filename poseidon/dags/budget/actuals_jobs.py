"""FM actuals _jobs file."""
import pandas as pd
import logging
import os
import glob
import re
import subprocess
from trident.util import general

conf = general.config
prod_path = conf['prod_data_dir']

def get_capital_ptd_act():
    """Get chart of accounts from shared drive."""
    logging.info('Retrieving latest CIP project to date')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;" \
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Actuals/Capital/P-T-D/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*ACTUALS.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_capital_act():
    """Get latest capital budgets from shared drive."""
    logging.info('Retrieving latest CIP project to date')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Actuals/Capital/FY/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*ACTUALS.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_operating_act():
    """Get latest operating budgets from shared drive."""
    logging.info('Retrieving latest CIP project')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Actuals/Operating/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*ACTUALS.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def create_capital_act():
    """ Use fy and p-t-d capital sets and ref sets to make capital actuals datasets """

    fy_final = glob.glob(conf['temp_data_dir'] \
            + "/FY*_FINAL_CIP_ACTUALS.xlsx")
    
    todate = glob.glob(conf['temp_data_dir'] \
                + "/FY*_2DATE_CIP_ACTUALS.xlsx")

    budgets = fy_final + todate

    fund_ref = pd.read_csv(prod_path \
        + "/budget_reference_funds_datasd.csv",dtype={'fund_number':str})
    proj_ref = pd.read_csv(prod_path \
        + "/budget_reference_projects_datasd.csv",dtype={'project_number':str})
    accounts_ref = pd.read_csv(prod_path \
        + "/budget_reference_accounts_datasd.csv",dtype={'account_number':str})

    for count, budget in enumerate(budgets):

        fy_pattern = re.compile(r'([0-9][0-9])')
        this_fy = fy_pattern.findall(budget)

        if "2DATE" in budget:
            out_fname = prod_path \
                + "/actuals_capital_ptd_FY{}_datasd.csv".format(this_fy[0])
        else:
            out_fname = prod_path \
                + "/actuals_capital_FY{}_datasd.csv".format(this_fy[0])

        df = pd.read_excel(budget)
        df = df.iloc[:, [0,1,2,3,4]]
        df.columns = ['amount','code','project_number_parent','project_number_child','object_number']
        df = df.fillna('')
        df['code'] = df['code'].astype(str)
        df['project_number_parent'] = df['project_number_parent'].astype(str)
        df['project_number_child'] = df['project_number_child'].astype(str)
        df['object_number'] = df['object_number'].astype(str)

        df = pd.merge(df,
            fund_ref[['fund_type','fund_number']],
            left_on='code',
            right_on='fund_number',
            how='left')
        df = pd.merge(df,
            proj_ref[['asset_owning_dept','project_name','project_number']],
            left_on='project_number_parent',
            right_on='project_number',
            how='left')
        df = pd.merge(df,
            accounts_ref[['account','account_number']],
            left_on='object_number',
            right_on='account_number',
            how='left')

        df = df[['amount',
        'fund_type',
        'fund_number',
        'asset_owning_dept',
        'project_name',
        'project_number_parent',
        'project_number_child',
        'account',
        'account_number']]

        general.pos_write_csv(df,out_fname)

    return "Successfully created capital actuals"

def create_operating_act():
    """ Use operating and ref sets to make operating dataset """

    budgets = glob.glob(conf['temp_data_dir'] \
            + "/FY*_FINAL_OM_ACTUALS.xlsx")

    for count, budget in enumerate(budgets):
        
        fy_pattern = re.compile(r'([0-9][0-9])')
        this_fy = fy_pattern.findall(budget)

        out_fname = prod_path \
        + "/actuals_operating_FY{}_datasd.csv".format(this_fy[0])

        df = pd.read_excel(budget)
        df = df.iloc[:, [0,1,2,3]]
        df.columns = ['amount','code','dept_number','commitment_item']
        df['code'] = df['code'].astype(str)
        df['dept_number'] = df['dept_number'].astype(str)
        df['commitment_item'] = df['commitment_item'].astype(str)

        fund_ref = pd.read_csv(prod_path \
            + "/budget_reference_funds_datasd.csv",dtype={'fund_number':str})
        depts_ref = pd.read_csv(prod_path \
            + "/budget_reference_depts_datasd.csv",dtype={'funds_center_number':str})
        accounts_ref = pd.read_csv(prod_path \
            + "/budget_reference_accounts_datasd.csv",dtype={'account_number':str})

        df = pd.merge(df,
            fund_ref[['fund_type','fund_number']],
            left_on='code',
            right_on='fund_number',
            how='left')
        df = pd.merge(df,
            depts_ref[['dept_name','funds_center_number']],
            left_on='dept_number',
            right_on='funds_center_number',
            how='left')
        df = pd.merge(df,
            accounts_ref[['account','account_number']],
            left_on='commitment_item',
            right_on='account_number',
            how='left')

        df = df[['amount',
        'fund_type',
        'fund_number',
        'dept_name',
        'funds_center_number',
        'account',
        'account_number']]

        general.pos_write_csv(df,out_fname)

    return "Successfully created operating actuals"