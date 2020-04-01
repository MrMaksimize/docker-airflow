"""FM actuals _jobs file."""
import pandas as pd
import logging
import glob
import re
from trident.util import general
from dags.budget.budget_jobs import process_df

conf = general.config
prod_path = conf['prod_data_dir']
fy_pattern = re.compile(r'([0-9][0-9])')

def create_capital_act():
    """ Use fy and p-t-d capital sets and ref sets to make capital actuals datasets """

    fy_final = glob.glob(conf['temp_data_dir'] \
            + "/FY*_FINAL_CIP_ACTUALS.xlsx")
    
    todate = glob.glob(conf['temp_data_dir'] \
                + "/FY*_2DATE_CIP_ACTUALS.xlsx")

    budgets = [
    {'files':fy_final,
    'out_fname':f"{prod_path}/actuals_capital_fy_datasd.csv"},
    {'files':todate,
    'out_fname':f"{prod_path}/actuals_capital_ptd_datasd.csv"}]

    fund_ref = pd.read_csv(prod_path \
        + "/budget_reference_funds_datasd_v1.csv",
        dtype={'fund_number':str})
    proj_ref = pd.read_csv(prod_path \
        + "/budget_reference_projects_datasd_v1.csv",
        dtype={'project_number':str})
    accounts_ref = pd.read_csv(prod_path \
        + "/budget_reference_accounts_datasd_v1.csv",
        dtype={'account_number':str})


    for budget in budgets:

        df_list = []
        
        for agg in budget.get('files'):
        
            cols = ['amount',
            'code',
            'project_number_parent',
            'project_number_child',
            'object_number']

            df = process_df(agg,cols)

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
            'account_number',
            'fiscal_year'
            ]]

            df_list.append(df)

        df_final = pd.concat(df_list)

        general.pos_write_csv(df_final,budget.get('out_fname'))

    return "Successfully created capital actuals"

def create_operating_act():
    """ Use operating and ref sets to make operating dataset """

    budgets = glob.glob(conf['temp_data_dir'] \
            + "/FY*_FINAL_OM_ACTUALS.xlsx")

    fund_ref = pd.read_csv(prod_path \
            + "/budget_reference_funds_datasd_v1.csv",
            dtype={'fund_number':str})
    depts_ref = pd.read_csv(prod_path \
        + "/budget_reference_depts_datasd_v1.csv",
        dtype={'funds_center_number':str})
    accounts_ref = pd.read_csv(prod_path \
        + "/budget_reference_accounts_datasd_v1.csv",
        dtype={'account_number':str})

    out_fname = f"{prod_path}/actuals_operating_datasd.csv"

    df_list = []

    for count, budget in enumerate(budgets):
        
        cols = ['amount',
        'code',
        'dept_number',
        'commitment_item']

        df = process_df(budget,cols)

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

        df_list.append(df)

    df_final = pd.concat(df_list)

    general.pos_write_csv(df_final,out_fname)

    return "Successfully created operating actuals"