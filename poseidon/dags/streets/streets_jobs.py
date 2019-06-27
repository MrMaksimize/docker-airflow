import os
import pandas as pd
import requests
import numpy as np
from datetime import datetime, timedelta
import logging
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general

conf = general.config

temp_file = conf['temp_data_dir'] + '/sd_paving_results.csv'

prod_file = {
    'sdif': conf['prod_data_dir'] + '/sd_paving_datasd_v1.csv',
    'imcat': conf['prod_data_dir'] + '/sd_paving_imcat_datasd_v1.csv'
}

def get_paving_miles(row):
    """ Calculate paving miles """
    
    if row['seg_width_ft'] > 50:
        return (row['seg_length_ft'] * 2)/5280
    else:
        return row['seg_length_ft']/5280


def get_streets_paving_data():
    """Get streets paving data from DB."""
    
    pv_query = general.file_to_string('./sql/pavement_ex.sql', __file__)
    pv_conn = MsSqlHook(mssql_conn_id='streets_cg_sql')

    df = pv_conn.get_pandas_df(pv_query)

    results = df.shape[0]

    general.pos_write_csv(
        df, temp_file)
    
    return f"Successfully wrote temp file with {results} records"

def process_paving_data(mode='sdif', **kwargs):

    """Get streets paving data from DB."""
    moratorium_string = "Post Construction"
    phone_UTLY = "858-627-3200"
    phone_OTHER = "619-527-7500"
    TSW_PM = "JLahmann@sandiego.gov"
    UTLY_PM = "Engineering@sandiego.gov"
    ACT_OVERLAY_CONCRETE_PM = "CHudson@sandiego.gov"
    ACT_SLURRY_SERIES_PM = "AVance@sandiego.gov"

    date_cols = ['wo_design_start_dt','wo_design_end_dt','job_start_dt','job_end_dt']

    df = pd.read_csv(temp_file,parse_dates=date_cols,low_memory=False)



    for i in [
            'seg_id', 'rd_seg_id', 'wo_id', 'wo_name', 'wo_status',
            'wo_proj_type', 'job_activity', 'seg_func_class'
    ]:

        df[i] = df[i].astype(str)


    # Backfill - set all fields to mora or construction    
    df.loc[(df.job_completed_cbox == 1), "wo_status"] = moratorium_string
    df.loc[(df.job_completed_cbox != 1) &
       (df.wo_status.str.contains('post construction|moratorium|post-construction',
        regex=True,
        case=False)), "wo_status"] = "Construction"


    # Remove Records w/o A Completed Date ONLY in the UTLY work order
    # IMCAT ONLY
    if mode == 'imcat':
        df = df[~((df.wo_id == "UTLY") & (df.job_end_dt.isnull()))]
        

        # Remove empty activities (IMCAT ONLY)
        df = df.query('not '\
                    + '(job_activity.isnull() '\
                    + '| job_activity == "" '\
                    + '| job_activity == "None")')

        

    # Remove Data Entry
    # Remove mill / pave
    # Remove Structure Widening
    # Remove Patching
    if mode == 'imcat':
        remove_search = 'data entry|mill|structure wid|patching'
    else:
        remove_search = 'data entry|structure wid|patching'

    df = df[~(df.job_activity.str.contains(
        remove_search, regex=True, case=False))]

    

    # Search Strings
    concrete_search = "panel rep|pcc - reconstruc"
    slurry_search = "surface treatment|scrub seal|cape seal|central mix"
    overlay_search = "resurfacing|overlay|ac overlay|mill|ac - overlay|ac - ac overlay|ac - reconstruct|ac inlay"

    # Set Proj Type to NA
    df['wo_proj_type'] = None
    # Concrete
    df.loc[df.job_activity.str.contains(
        concrete_search, regex=True, case=False), 'wo_proj_type'] = 'Concrete'
    # Slurry
    df.loc[df.job_activity.str.contains(
        slurry_search, regex=True, case=False), 'wo_proj_type'] = 'Slurry'
    # Overlay
    df.loc[df.job_activity.str.contains(
        overlay_search, regex=True, case=False), 'wo_proj_type'] = 'Overlay'

    # Create ref dates
    today = general.today()
    five_yrs_ago = today.replace(year=(today.year - 5))
    three_yrs_ago = today.replace(year=(today.year - 3))

    # Remove records
    df = df[(df.job_end_dt > five_yrs_ago) | (df.job_end_dt.isnull())]

    # Remove Slurry Records > 3 Years Old
    # IMCAT ONLY
    if mode == 'imcat':
        mask = ~((df.wo_proj_type == 'Slurry') &
                 (df.job_end_dt < three_yrs_ago))
        df = df[mask]

        

    # Set all completed jobs to Moratorium status
    df.loc[(df.job_completed_cbox == 1),
           "wo_status"] = moratorium_string

    # Set Dates in The future for TSW work orders as Construction.
    mask = (df.wo_id == 'TSW') & \
           (df.job_end_dt.notnull()) & \
           (df.job_end_dt > today)

    df.loc[mask, "wo_status"] = "Construction"

    # Set other TSW works orders as Construction
    df.loc[(df.wo_id == "TSW") & 
          (df.job_completed_cbox == 0),'wo_status'] = "Construction"

    # Set Phone # For UTLY
    df.loc[df.wo_id == 'UTLY', 'wo_pm_phone'] = phone_UTLY

    # Set Phone # for Everything else
    df.loc[df.wo_id != 'UTLY', 'wo_pm_phone'] = phone_OTHER

    # Set PM for UTLY
    df.loc[df.wo_id == 'UTLY', 'wo_pm'] = UTLY_PM

    # Set PM for TSW
    df.loc[df.wo_id == 'TSW', 'wo_pm'] = TSW_PM

    # Set PM for Overlay / Concrete
    #mask = (df.wo_proj_type == 'Overlay') | (df.wo_proj_type == 'Concrete') & (df.wo_pm.isnull())
    mask = (df.wo_pm.isnull()) & ((df.wo_proj_type == 'Overlay') |
                                  (df.wo_proj_type == 'Concrete'))
    df.loc[mask, 'wo_pm'] = ACT_OVERLAY_CONCRETE_PM

    # Set PM for Slurry / Series
    mask = (df.wo_pm.isnull()) & ((df.wo_proj_type == 'Slurry') |
                                  (df.wo_proj_type == 'Series Circuit'))
    df.loc[mask, 'wo_pm'] = ACT_SLURRY_SERIES_PM

    # Spot Unknown
    mask = (df.job_activity.isnull()) | (df.job_activity == None) | (df.job_activity == 'None') | (df.job_activity == '')\
        |(df.wo_proj_type.isnull()) | (df.wo_proj_type == None) | (df.wo_proj_type == 'None') | (df.wo_proj_type == '')\
        |(df.wo_status.isnull()) | (df.wo_status == None) | (df.wo_status == 'None') | (df.wo_status == '')

    spot_unknown = df[mask]

    logging.info('Found {} records with no activity, type or status'.format(
        spot_unknown.shape[0]))

    # Remove unknown
    df = df[~mask]

    

    # Create separate moratorium column based on job end dt
    df['moratorium'] = df['job_end_dt']
    
    # But do not set moratorium for concrete
    df.loc[df.wo_proj_type == 'Concrete','moratorium'] = None
    df.loc[df.wo_status != moratorium_string,'moratorium'] = None
    
    # Start column is the wo_design_start only when job_completed_cbox is not checked
    df['start'] = df['wo_design_start_dt']
    df.loc[df.job_completed_cbox == 1,'start'] = df.loc[df.job_completed_cbox == 1,'job_start_dt']

    # End column is the wo_design_end only when job_completed_cbox is not checked
    df['end'] = df['wo_design_end_dt']
    df.loc[df.job_completed_cbox == 1,'end'] = df.loc[df.job_completed_cbox == 1,'job_end_dt']

    # Sort by job end date time
    df = df.sort_values(by='job_end_dt', na_position='last', ascending=False)

    # Remove duplicates, although it doesn't make sense
    # This is wrong.
    df = df.drop_duplicates('seg_id', keep='first')



    # Now that start and end columns are correct, remove other date columns
    df = df.drop(columns=['wo_design_start_dt','wo_design_end_dt','job_start_dt','job_end_dt'])

    # Calculate paving miles
    paving_miles = df.apply(get_paving_miles, axis=1)
    df = df.assign(paving_miles=paving_miles)

    # For IMCAT uppercase status
    if mode == 'imcat':


        df.columns = ['PVE_ID','SEG_ID','RD_SEG_ID','PROJECTID','TITLE','PM',
        'PM_PHONE','COMPLETED','STATUS','PROJ_TYPE','ACTIVITY','RESIDENT_ENGINEER',
        'STREET','STREET_FROM','STREET_TO','ENTRY_DT','LAST_UPDATE','SEG_IN_SERV',
        'SEG_FUN_CLASS','SEG_CD','LENGTH','WIDTH','MORATORIUM','START','END','PAVING_MILES']

        df['STATUS'] = df['STATUS'].str.upper()

    else:

        # Drop additional columns for public dataset

        df = df.drop(columns=['rd_seg_id',
            'job_completed_cbox',
            'job_activity',
            'job_entry_dt',
            'job_updated_dt',
            'seg_placed_in_srvc',
            'seg_func_class',
            'seg_council_district'
            ])

        df.columns = ['pve_id','seg_id','project_id','title','project_manager',
        'project_manager_phone','status','type','resident_engineer','address_street',
        'street_from','street_to','length','width','date_moratorium',
        'date_start','date_end','paving_miles']

        df['status'] = df['status'].str.lower()

    
    # Write csv
    logging.info('Writing ' + str(df.shape[0]) + ' rows in mode ' + mode)
    general.pos_write_csv(
        df, prod_file[mode], date_format=conf['date_format_ymd_hms'])
    
    return "Successfully wrote prod file at " + prod_file[mode]


def build_sonar_miles_aggs(mode='sdif', pav_type='total', **kwargs):
    pav_csv = prod_file[mode]
    dbl_spec = 2

    range_start = kwargs['range_start']
    range_start_year = range_start.year
    range_start_month = range_start.month
    range_start_day = range_start.day

    range_start_naive = datetime(range_start_year,range_start_month,range_start_day)

    # Read CSV
    df = pd.read_csv(pav_csv)


    # Convert moratorium to date
    df["moratorium"] = pd.to_datetime(df["moratorium"])

    # Get post construction, within range
    mask = (df.status == 'Post Construction') & \
           (df.moratorium >= range_start_naive)
    df = df[mask]

    # Get sums
    sums = df[["paving_miles", "type"]].groupby("type").sum()
    sums.reset_index(inplace=True)

    # Get total paved
    total = round(sums["paving_miles"].sum(), dbl_spec)

    # Get total overlay
    overlay = sums.loc[sums["type"] == 'Overlay', "paving_miles"].reset_index()

    if len(overlay) == 0:
        overlay = 0
    else:
        overlay = round(overlay["paving_miles"][0], dbl_spec)

    # Get total slurry
    slurry = sums.loc[sums["type"] == 'Slurry', "paving_miles"].reset_index()
    if len(slurry) == 0:
        slurry = 0
    else:
        slurry = round(slurry["paving_miles"][0], dbl_spec)


    # Return dicts
    if pav_type == 'total':
        logging.info('{} miles paved {}'.format(pav_type, total))
        return {'value': total}
    elif pav_type == 'overlay':
        logging.info('{} miles paved {}'.format(pav_type, overlay))
        return {'value': overlay}
    elif pav_type == 'slurry':
        logging.info('{} miles paved {}'.format(pav_type, slurry))
        return {'value': slurry}
    else:
        raise ValueError("Unknown pav_type")
