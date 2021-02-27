"""FM budget _jobs file."""
import pandas as pd
import logging
import os
import glob
import re
from subprocess import Popen, PIPE, check_output
import subprocess
from trident.util import general
from shlex import quote
from airflow.hooks.base_hook import BaseHook

conf = general.config
prod_path = conf['prod_data_dir']
fy_pattern = re.compile(r'([0-9][0-9])')

def get_accounts_chart():
    """Get chart of accounts from shared drive."""
    logging.info('Retrieving chart of accounts file.')
    conn = BaseHook.get_connection(conn_id="SVC_ACCT")
    command = "smbclient //ad.sannet.gov/dfs " \
        + f"--user={conn.login}%{conn.password} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/\";" \
        + " lcd \"/data/temp/\";" \
        + " mget *.xlsx'"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(error)
    else:
        logging.info("Found file")
        return "Downloaded chart of accounts"

def get_budget_files(mode='', path=''):
    """Get latest actuals from shared drive."""
    logging.info(f'Retrieving {mode} for {path}')
    conn = BaseHook.get_connection(conn_id="SVC_ACCT")
    command = "smbclient //ad.sannet.gov/dfs " \
        + f"--user={conn.login}%{conn.password} -W ad -c " \
        + "'prompt OFF;"\
        + " cd \"FMGT-Shared/Shared/BUDGET/" \
        + "Open Data/Open Data Portal/" \
        + "Shared with Performance and Analytics/" \
        + f"{mode}/{path}/\";" \
        + " lcd \"/data/temp/\";" \
        + f" mget FY*{mode.upper()}.xlsx;'"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(error)
    else:
        logging.info("Found file")
        return f"Downloaded {mode} {path}"

def get_ref_sets():
    """ Get chart of accounts and make ref sets """

    logging.info('Retrieving tabs in chart of accounts')

    # Pull in 5 tabs from chart of accounts
    # Use these to make reference datasets

    filename = conf['temp_data_dir'] + "/*Chart of Accounts*.xlsx"
    list_of_files = glob.glob(filename)
    logging.info(len(list_of_files))
    latest_file = max(list_of_files, key=os.path.getmtime)
    logging.info(f"Reading in {latest_file}")

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
        file = pd.read_excel(latest_file,
            engine='openpyxl',
            sheet_name=sheet,
            dtype={'(code)':str})
        logging.info(f"Read sheet {sheet}")
        files.append(file)
        logging.info("Successfully read sheet and appended")

    # For each sheet, get subsets of columns

    funds = files[0].loc[:, "Fund Type":"(code)"]
    assets_depts_subset = files[1].loc[:, ["Asset Owning Department","(code)"]]
    assets_types_subset = files[2].loc[:, "Asset Type/Project":"(code)"]
    depts = files[3].loc[:, "Department Group":"(code)"]
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

def create_file(mode='',path=''):
    """ Create actuals and budget for operating, 
    capital ptd and capital fy"""
    
    in_cols = ['amount','code']

    file_pattern = []

    fp_pre = "/FY*_"

    if path == "capital_ptd":
        merges = ['fund', 'proj', 'accounts']
        file_pattern.append(f"{fp_pre}2DATE_CIP_{mode.upper()}")
        in_cols.extend(['project_number_parent','project_number_child','object_number','fiscal_year'])
    elif path == "capital_fy":
        merges = ['fund', 'proj', 'accounts']
        if mode  == 'actuals':
            file_pattern.append(f"{fp_pre}FINAL_CIP_{mode.upper()}")
            in_cols.extend(['project_number_parent','project_number_child','object_number'])
        else:
            file_pattern.extend([f"{fp_pre}PROP_CIP_{mode.upper()}",f"{fp_pre}ADOPT_CIP_{mode.upper()}"])
            in_cols.extend(['project_number','object_number'])
    else:
        merges = ['fund','depts','accounts']
        file_pattern.append(f"{fp_pre}OM_{mode.upper()}")
        in_cols.extend(['dept_number','object_number'])

    budgets = []

    logging.info(f"Getting list of files for {mode} {path}")
    
    for fp in file_pattern:
        budget_files = glob.glob(f"{conf['temp_data_dir']}/{fp}.xlsx")
        budgets.extend(budget_files)
    
    df_list = []

    logging.info("Reading in individual files")

    for budget in budgets:

        df_temp = process_df(budget,in_cols)
                
        df_list.append(df_temp)

    logging.info("Concatting individual files")

    df = pd.concat(df_list)

    logging.info("Performing merges")

    df_final = merge_df(df,merges)

    out_fname = f"{prod_path}/{mode}_{path}_datasd.csv"

    logging.info(f"Writing final file to {out_fname}")

    general.pos_write_csv(df_final,out_fname)

    file_specs = {'budget_capital_ptd':{
            'file_pattern':[f"FY*_2DATE_CIP_{mode.upper()}"],
            'refs':['fund','proj','accounts'],
            'in_cols':['amount',
                        'code',
                        'project_number_parent',
                        'project_number_child',
                        'object_number',
                        'fiscal_year'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'asset_owning_dept',
                        'project_name',
                        'project_number_parent',
                        'project_number_child',
                        'account',
                        'account_number',
                        'fiscal_year',
                        'budget_cycle'],
            'out_fname':f"{prod_path}/budget_capital_ptd_datasd.csv"
        },
        'budget_capital_fy':{
            'file_pattern':[f"/FY*_PROP_CIP_{mode.upper()}",f"/FY*_ADOPT_CIP_{mode.upper()}"],
            'refs':['fund', 'proj', 'accounts'],
            'in_cols':['amount',
                        'code',
                        'project_number',
                        'object_number'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'asset_owning_dept',
                        'project_name',
                        'project_number',
                        'account',
                        'account_number',
                        'fiscal_year',
                        'budget_cycle'],
            'out_fname':f"{prod_path}/budget_capital_fy_datasd.csv"
        },
        'budget_operating':{
            'file_pattern':[f"/FY*_OM_{mode.upper()}"],
            'refs':['fund','depts','accounts'],
            'in_cols':['amount',
                        'code',
                        'dept_number',
                        'commitment_item'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'dept_name',
                        'funds_center_number',
                        'account',
                        'account_number',
                        'fiscal_year',
                        'budget_cycle'],
            'out_fname':f"{prod_path}/budget_operating_datasd.csv"
        },
        'actuals_capital_ptd':{
            'file_pattern':[f"FY*_2DATE_CIP_{mode.upper()}"],
            'refs':['fund','proj','child_proj','accounts'],
            'in_cols':['amount',
                        'code',
                        'project_number_parent',
                        'project_number_child',
                        'object_number',
                        'fiscal_year'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'asset_owning_dept',
                        'project_name',
                        'project_number_parent',
                        'project_number_child',
                        'account',
                        'account_number',
                        'fiscal_year'],
            'out_fname':f"{prod_path}/actuals_capital_ptd_datasd.csv",
        },
        'actuals_capital_fy':{
            'file_pattern':[f"/FY*_FINAL_CIP_{mode.upper()}"],
            'refs':['fund','proj','child_proj','accounts'],
            'in_cols':['amount',
                        'code',
                        'project_number_parent',
                        'project_number_child',
                        'object_number'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'asset_owning_dept',
                        'project_name',
                        'project_number_parent',
                        'project_number_child',
                        'account',
                        'account_number',
                        'fiscal_year'],
            'out_fname':f"{prod_path}/actuals_capital_fy_datasd.csv"
        },
        'actuals_operating':{
            'file_pattern':[f"/FY*_OM_{mode.upper()}"],
            'refs':['fund','depts','accounts'],
            'in_cols':['amount',
                        'code',
                        'dept_number',
                        'commitment_item'],
            'out_cols':['amount',
                        'fund_type',
                        'fund_number',
                        'dept_name',
                        'funds_center_number',
                        'account',
                        'account_number',
                        'fiscal_year'],
            'out_fname':f"{prod_path}/actuals_operating_datasd.csv"
        }
    }

    return "Successfully created file"

#: Helper function
def merge_df(df,merges):
    """ Determine/perform merges and return finalized df """
    
    if "fund" in merges:
        df = funds_merge(df)
    if "proj" in merges:
        df = projs_merge(df)
    if "depts" in merges:
        df = depts_merge(df)
    if "accounts" in merges:
        df = accounts_merge(df)

    return df

#: Helper function
def funds_merge(df):
    """ Merge financials with funds """

    fund_ref = pd.read_csv(prod_path \
        + "/budget_reference_funds_datasd_v1.csv",dtype={'fund_number':str})

    logging.info(f"Starting funds merge with {df.shape[0]} rows")

    df = pd.merge(df,
        fund_ref[['fund_type','fund_number']],
        left_on='code',
        right_on='fund_number',
        how='left')

    logging.info(f"Ending departments merge with {df.shape[0]} rows")

    df = df.drop(columns=['code'])

    return df

#: Helper function  
def projs_merge(df):
    """ Merge financials with projects """

    proj_ref = pd.read_csv(prod_path \
        + "/budget_reference_projects_datasd_v1.csv",dtype={'project_number':str})

    orig_cols = df.columns.tolist()

    if 'project_number_parent' in orig_cols:
        left_on = 'project_number_parent'
        col_drop = ['project_number']
    else:
        left_on = 'project_number'
        col_drop = []

    logging.info(f"Starting projects merge with {df.shape[0]} rows")

    df = pd.merge(df,
        proj_ref[['asset_owning_dept','project_name','project_number']],
        left_on=left_on,
        right_on='project_number',
        how='left')

    logging.info(f"Ending projects merge with {df.shape[0]} rows")

    df = df.drop(columns=col_drop)

    return df

#: Helper function
def accounts_merge(df):
    """ Merge financials with accounts """

    accounts_ref = pd.read_csv(prod_path \
        + "/budget_reference_accounts_datasd_v1.csv",dtype={'account_number':str})

    logging.info(f"Starting accounts merge with {df.shape[0]} rows")

    df = pd.merge(df,
        accounts_ref[['account','account_number']],
        left_on='object_number',
        right_on='account_number',
        how='left')

    logging.info(f"Ending accounts merge with {df.shape[0]} rows")

    df = df.drop(columns=['object_number'])

    return df

#: Helper function
def depts_merge(df):
    """ Merge financials with departments """

    depts_ref = pd.read_csv(prod_path \
        + "/budget_reference_depts_datasd_v1.csv",dtype={'funds_center_number':str})

    logging.info(f"Starting departments merge with {df.shape[0]} rows")

    df = pd.merge(df,
        depts_ref[['dept_name','funds_center_number']],
        left_on='dept_number',
        right_on='funds_center_number',
        how='left')

    logging.info(f"Ending departments merge with {df.shape[0]} rows")

    df = df.drop(columns=['dept_number'])

    return df

#: Helper function
def process_df(filepath,cols):
    """ Peform processing common to all sets """

    this_fy = fy_pattern.findall(filepath)

    df = pd.read_excel(filepath,engine='openpyxl')
    
    # Some files come through with blank, unnamed cols

    read_cols = df.columns.tolist()
    if 'Fiscal Year' in read_cols:
        # This is the last col in CIP PTD stuff
        last_col = read_cols.index('Fiscal Year') + 1
    elif 'Object Number' in read_cols:
        # This is the last col in most everything else
        last_col = read_cols.index('Object Number') + 1
    elif 'Commitment Item Number' in read_cols:
        # One operating budget file has this col
        last_col = read_cols.index('Commitment Item Number') + 1
    else:
        # default
        last_col = len(read_cols)
    
    col_select = read_cols[0:last_col]


    df = df.loc[:,col_select]

    df.columns = cols

    df.loc[:,cols[1:]] = df.loc[:,cols[1:]].fillna('')
    df.loc[:,cols[1:]] = df.loc[:,cols[1:]].astype(str)

    df['report_fy'] = this_fy[0]

    # Don't need this for Actuals
    if "PROP" in filepath or "ADOPT" in filepath:
        if "PROP" in filepath:
            df['budget_cycle'] = 'proposed'
        else:
            df['budget_cycle'] = 'adopted'

    return df