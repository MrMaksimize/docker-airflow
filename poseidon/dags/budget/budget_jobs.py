"""FM budget _jobs file."""
import pandas as pd
import logging
import os
import glob
import re
import subprocess
from trident.util import general

conf = general.config
prod_path = conf['prod_data_dir']


def get_accounts_chart():
    """Get chart of accounts from shared drive."""
    logging.info('Retrieving chart of accounts file.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/\";" \
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

def get_capital_ptd():
    """Get chart of accounts from shared drive."""
    logging.info('Retrieving latest CIP project to date')
    #+ " lcd \"/data/sd_airflow/poseidon/data/temp\";" \
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Budget/Capital/P-T-D/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*BUDGET.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_capital():
    """Get latest capital budgets from shared drive."""
    logging.info('Retrieving latest CIP project to date')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Budget/Capital/FY/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*BUDGET.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_operating():
    """Get latest operating budgets from shared drive."""
    logging.info('Retrieving latest operating budget')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + "Budget/Operating/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget FY*BUDGET.xlsx;'"

    command = command.format(adname=conf['alb_sannet_user'],
                             adpass=conf['alb_sannet_pass'],
                             temp_dir=conf['temp_data_dir'])

    logging.info(command)

    try:
        p = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as e:
        return e.output

def get_ref_sets():
    """ Get chart of accounts and make ref sets """

    logging.info('Retrieving tabs in chart of accounts')

    # Pull in 5 tabs from chart of accounts
    # Use these to make reference datasets

    temp_files = [f for f in os.listdir(conf['temp_data_dir'])]

    for f in temp_files:
        
        if 'Chart of Accounts' in f:

            accounts_path = conf['temp_data_dir'] + '/' + f

    sheets = [
    "Funds",
    "Asset Owning Department",
    "Asset Type Project",
    "Departments Programs",
    "Expenses",
    "Revenues"
    ]

    files = []

    for sheet in sheets:
        file = pd.read_excel(accounts_path,sheet_name=sheet)
        logging.info("Read sheet "+sheet)
        file['(code)'] = file['(code)'].astype(str)
        files.append(file)
        logging.info("Successfully read sheet and appended")

    # For each sheet, get subsets of columns

    funds = files[0].loc[:, "Fund Type":"(code)"]
    assets_depts_subset = files[1].loc[:, ["Asset Owning Department","(code)"]]
    assets_types_subset = files[2].loc[:, "Asset Type/Project":"(code)"]
    depts = files[3].loc[:, ["Department Group",
    "Department",
    "Division",
    "Section",
    "Fund Center",
    "(code)"]]
    exp_subset = files[4].loc[:, "Object Type":"(code)"]
    rev_subset = files[5].loc[:, "Object Type":"(code)"]

    logging.info("Successfully created subsets from each sheet")

    funds.columns = [
    "fund_type",
    "fund_name",
    "fund_number"
    ]

    logging.info("Funds reference made")

    projects = pd.merge(
        assets_depts_subset, 
        assets_types_subset, 
        on='(code)', 
        how='outer')

    projects.columns = [
    "asset_owning_dept",
    "project_number",
    "asset_type",
    "asset_subtype",
    "project_name"
    ]

    logging.info("Projects reference made")

    depts.columns = [
    "dept_group",
    "dept_name",
    "dept_division",
    "dept_section",
    "funds_center",
    "funds_center_number"
    ]

    logging.info("Departments reference made")

    items = pd.concat([exp_subset,rev_subset])
    items.columns = [
    "account_type",
    "account_class",
    "account_group",
    "account",
    "account_number"
    ]

    logging.info("Items reference made")

    logging.info("Writing reference sets")

    general.pos_write_csv(funds,prod_path \
        + "/budget_reference_funds_datasd_v1.csv")
    general.pos_write_csv(projects,prod_path \
        + "/budget_reference_projects_datasd_v1.csv")
    general.pos_write_csv(depts,prod_path \
        + "/budget_reference_depts_datasd_v1.csv")
    general.pos_write_csv(items,prod_path \
        + "/budget_reference_accounts_datasd_v1.csv")

    return "Successfully created reference sets"
        

def create_capital():
    """ Use fy and p-t-d capital sets and ref sets to make capital datasets """

    adopted = glob.glob(conf['temp_data_dir'] \
            + "/FY*_ADOPT_CIP_BUDGET.xlsx")
    proposed = glob.glob(conf['temp_data_dir'] \
                + "/FY*_PROP_CIP_BUDGET.xlsx")
    todate = glob.glob(conf['temp_data_dir'] \
                + "/FY*_2DATE_CIP_BUDGET.xlsx")

    budgets = adopted + proposed + todate

    fund_ref = pd.read_csv(prod_path \
        + "/budget_reference_funds_datasd_v1.csv",dtype={'fund_number':str})
    proj_ref = pd.read_csv(prod_path \
        + "/budget_reference_projects_datasd_v1.csv",dtype={'project_number':str})
    accounts_ref = pd.read_csv(prod_path \
        + "/budget_reference_accounts_datasd_v1.csv",dtype={'account_number':str})

    for count, budget in enumerate(budgets):
        fy_pattern = re.compile(r'([0-9][0-9])')
        this_fy = fy_pattern.findall(budget)

        if "2DATE" in budget:
            out_fname = prod_path \
                + "/budget_capital_ptd_FY{}_datasd_v1.csv".format(this_fy[0])
        elif "PROP" in budget:
            out_fname = prod_path \
                + "/budget_capital_FY{}_prop_datasd_v1.csv".format(this_fy[0])
        else:
            out_fname = prod_path \
                + "/budget_capital_FY{}_datasd_v1.csv".format(this_fy[0])

        df = pd.read_excel(budget)
        df = df.iloc[:, [0,1,2,3]]
        df.columns = ['amount','code','project_number','object_number']
        df['code'] = df['code'].astype(str)
        df['project_number'] = df['project_number'].astype(str)
        df['object_number'] = df['object_number'].astype(str)

        df = pd.merge(df,
            fund_ref[['fund_type','fund_number']],
            left_on='code',
            right_on='fund_number',
            how='left')
        df = pd.merge(df,
            proj_ref[['asset_owning_dept','project_name','project_number']],
            left_on='project_number',
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
        'project_number',
        'account',
        'account_number']]

        general.pos_write_csv(df,out_fname)

    return "Successfully created capital budgets"

def create_operating():
    """ Use operating and ref sets to make operating dataset """

    adopted = glob.glob(conf['temp_data_dir'] \
            + "/FY*_ADOPT_OM_BUDGET.xlsx")
    proposed = glob.glob(conf['temp_data_dir'] \
                + "/FY*_PROP_OM_BUDGET.xlsx")

    budgets = adopted + proposed
    
    for count, budget in enumerate(budgets):
        
        fy_pattern = re.compile(r'([0-9][0-9])')
        this_fy = fy_pattern.findall(budget)

        if "PROP" in budget:
            out_fname = prod_path \
                + "/budget_operating_FY{}_prop_datasd_v1.csv".format(this_fy[0])
        else:
            out_fname = prod_path \
            + "/budget_operating_FY{}_datasd_v1.csv".format(this_fy[0])

        df = pd.read_excel(budget)
        df = df.iloc[:, [0,1,2,3]]
        df.columns = ['amount','code','dept_number','commitment_item']
        df['code'] = df['code'].astype(str)
        df['dept_number'] = df['dept_number'].astype(str)
        df['commitment_item'] = df['commitment_item'].astype(str)

        fund_ref = pd.read_csv(prod_path \
            + "/budget_reference_funds_datasd_v1.csv",dtype={'fund_number':str})
        depts_ref = pd.read_csv(prod_path \
            + "/budget_reference_depts_datasd_v1.csv",dtype={'funds_center_number':str})
        accounts_ref = pd.read_csv(prod_path \
            + "/budget_reference_accounts_datasd_v1.csv",dtype={'account_number':str})

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

    return "Sucessfully created operating budget"