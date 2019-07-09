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

def number_str_cols(col):
    col = col.fillna(-9999.0)
    col = col.astype(int)
    col = col.astype(str)
    col = col.replace('-9999', '')

    return col

def get_paving_miles(row):
    """ Calculate paving miles """
    
    if row['seg_width_ft'] >= 50:
        return (row['seg_length_ft'] * 2)/5280
    else:
        return row['seg_length_ft']/5280

def get_start_end_dates(row):
    """ Determine correct start and end dates """

    if row['wo_id'] == 'UTLY' or row['wo_id'] == 'TSW':
        return row['job_start_dt'], row['job_end_dt']

    else:

        if row['job_completed_cbox'] == 1:
            return row['job_end_dt'], row['job_end_dt']

        else:
            return row['start'], row['end']

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
    ACT_SERIES_CIRCUIT_PM = "CHoenes@sandiego.gov"

    today = general.today()

    date_cols = ['wo_design_start_dt','wo_design_end_dt','job_start_dt','job_end_dt']

    df = pd.read_csv(temp_file,low_memory=False, parse_dates=date_cols)

    # Update column types

    float_cols = ['pve_id','rd_seg_id','seg_council_district']

    for i in float_cols:
        df[i] = number_str_cols(df[i])

    str_cols = ['seg_id','wo_proj_type','wo_id','wo_name']

    df.loc[:,str_cols] = df.loc[:,str_cols].replace(np.nan, '')

    #*** Update project types for consistency ***

    logging.info("Updating project type to Concrete, Slurry or Overlay")

    # Search Strings
    concrete_search = "panel rep|pcc - reconstruc"
    slurry_search = "surface treatment|scrub seal|cape seal|central mix"
    overlay_search = "resurfacing|overlay|ac overlay|mill|ac - overlay|ac - ac overlay|ac - reconstruct|ac inlay"

    # Set Proj Type to NA
    df['wo_proj_type'] = None
    # Concrete
    df.loc[df.job_activity.str.contains(
        concrete_search, regex=True, case=False, na=False), 'wo_proj_type'] = 'Concrete'
    # Slurry
    df.loc[df.job_activity.str.contains(
        slurry_search, regex=True, case=False, na=False), 'wo_proj_type'] = 'Slurry'
    # Overlay
    df.loc[df.job_activity.str.contains(
        overlay_search, regex=True, case=False, na=False), 'wo_proj_type'] = 'Overlay'

    #*** Update job status ***

    logging.info(f"Creating new status column with fixed status")

    df['status'] = df['wo_status']

    df.loc[(df.job_completed_cbox == 1), "status"] = moratorium_string
    
    df.loc[(df.job_completed_cbox != 1) &
       (df.wo_status.str.contains('post construction|moratorium|post-construction',
        regex=True,
        case=False)), "status"] = "Construction"

    # Set Dates in The future for TSW work orders as Construction.
    df.loc[(df.wo_id == 'TSW') &
           (df.job_end_dt.notnull()) &
           (df.job_end_dt > today), "status"] = "Construction"

    # Set other TSW works orders as Construction
    df.loc[(df.wo_id == "TSW") & 
          (df.job_completed_cbox == 0),'status'] = "Construction"

    #*** Update project manager ***

    logging.info(f"Updating project manager names and phone")

    # Set Phone # For UTLY
    df.loc[df.wo_id == 'UTLY', 'wo_pm_phone'] = phone_UTLY

    # Set Phone # for Everything else
    df.loc[df.wo_id != 'UTLY', 'wo_pm_phone'] = phone_OTHER

    # Set PM for UTLY
    df.loc[df.wo_id == 'UTLY', 'wo_pm'] = UTLY_PM

    # Set PM for TSW
    df.loc[df.wo_id == 'TSW', 'wo_pm'] = TSW_PM

    # Set PM for Overlay / Concrete
    df.loc[(df.wo_pm.isnull()) & 
        ((df.wo_proj_type == 'Overlay') |
        (df.wo_proj_type == 'Concrete')), 'wo_pm'] = ACT_OVERLAY_CONCRETE_PM

    # Set PM for Slurry
    df.loc[((df.wo_pm.isnull()) & 
        (df.wo_proj_type == 'Slurry')), 'wo_pm'] = ACT_SLURRY_SERIES_PM

    # Set PM for Series 
    df.loc[((df.wo_pm.isnull()) & 
        (df.wo_proj_type == 'Series Circuit')), 'wo_pm'] = ACT_SERIES_CIRCUIT_PM

    #*** Update moratorium, start and end dates ***

    logging.info(f"Updating moratorium, start, and end dates")
    
    # Create separate moratorium column based on job end dt
    df['moratorium'] = df['job_end_dt']
    
    # But do not set moratorium for concrete
    df.loc[df.wo_proj_type == 'Concrete','moratorium'] = None
    df.loc[df.status != moratorium_string,'moratorium'] = None
    
    # Start/end column is by default the wo_design_start/wo_design_end 
    df['start'] = df['wo_design_start_dt']
    df['end'] = df['wo_design_end_dt']

    # But here, we get an update based on a few criteria
    new_dates = df.apply(get_start_end_dates,axis=1)
    dates_final = new_dates.apply(pd.Series)
    df['start'] = dates_final[0]
    df['end'] = dates_final[1]

    #*** Calculate paving miles ***
    
    paving_miles = df.apply(get_paving_miles, axis=1)
    df = df.assign(paving_miles=paving_miles)

    #*** Remove unneeded records ***

    logging.info(f"Starting with {df.shape[0]} rows before removing records")

    # UTLY jobs where job end date is missing
    df = df[~((df.wo_id == "UTLY") & (df.job_end_dt.isnull()))]

    # Records for data entry, mill / pave, structure widening, and patching
    remove_search = 'data entry|mill|structure wid|patching'
    df = df[~(df.job_activity.str.contains(
        remove_search, regex=True, case=False, na=False))]    

    five_yrs_ago = today.replace(year=(today.year - 5))
    three_yrs_ago = today.replace(year=(today.year - 3))

    # Older than 5 years for both datasets
    df = df[(df.job_end_dt > five_yrs_ago) | (df.job_end_dt.isnull())]

    # Plus slurry records older than 3 years for imcat
    if mode == 'imcat':
        df = df[~((df.wo_proj_type == 'Slurry') &
                 (df.job_end_dt < three_yrs_ago))]

    # Records with no activity, type or status
    mask = (df.job_activity.isnull()) | (df.job_activity == None) | (df.job_activity == 'None') | (df.job_activity == '')\
        |(df.wo_proj_type.isnull()) | (df.wo_proj_type == None) | (df.wo_proj_type == 'None') | (df.wo_proj_type == '')\
        |(df.wo_status.isnull()) | (df.wo_status == None) | (df.wo_status == 'None') | (df.wo_status == '')

    spot_unknown = df[mask]

    logging.info('Found {} records with no activity, type or status'.format(
        spot_unknown.shape[0]))

    # Remove unknown
    df = df[~mask]

    logging.info(f"End with {df.shape[0]} rows after removing records")

    #*** Rename columns and create subsets ***

    # For IMCAT uppercase status
    if mode == 'imcat':

        logging.info("Flagging duplicates for removal")

        duplicates = []
        df = df.sort_values(by=['seg_id','job_end_dt'], na_position='first', ascending=[True,False])
        df_seg_groups = df.groupby(['seg_id'])
        for name, group in df_seg_groups:
            if group.shape[0] > 1:
                selection = group.loc[group['moratorium'].notnull()]
                if selection.shape[0] > 1:
                    index_list = selection.index.tolist()
                    select_remove = index_list[1:]
                    duplicates.extend(select_remove)

        df['to_delete'] = 0
        df.loc[duplicates,['to_delete']] = 1
        logging.info(duplicates)
        df = df.rename(columns={'wo_id':'projectid',
            'wo_name':'title',
            'wo_pm':'pm',
            'wo_pm_phone':'pm_phone',
            'job_completed_cbox':'completed',
            'wo_proj_type':'proj_type',
            'job_activity':'activity',
            'wo_resident_engineer':'resident_engineer',
            'seg_placed_in_srvc':'seg_in_serv',
            'seg_func_class':'seg_fun_class',
            'seg_length_ft':'length',
            'seg_width_ft':'width',
            'seg_council_district':'seg_cd',
            'job_entry_dt':'entry_dt',
            'seg_func_class':'seg_fun',
            'job_updated_dt':'last_update'
            })

        final_cols = ['to_delete','pve_id','rd_seg_id','seg_id','projectid','title',
        'pm','pm_phone','moratorium','status','proj_type','resident_engineer',
        'start','end','completed','job_start_dt','job_end_dt',
        'wo_design_start_dt','wo_design_end_dt','wo_status','activity',
        'entry_dt','last_update','street','street_from','street_to',
        'seg_fun','seg_cd','length','width','seg_in_serv','paving_miles']

        df_final = df[final_cols].copy()

        df_final.columns = [x.upper() for x in df_final.columns]
        df_final['STATUS'] = df_final['STATUS'].str.upper()

    else:

        # Remove duplicates
        df = df.sort_values(by='job_end_dt', na_position='last', ascending=False)
        df = df.drop_duplicates('seg_id', keep='first')

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

        df = df.rename(columns={'wo_id':'project_id',
            'wo_name':'title',
            'wo_pm':'project_manager',
            'wo_pm_phone':'project_manager_phone',
            'wo_proj_type':'type',
            'wo_resident_engineer':'resident_engineer',
            'street':'address_street',
            'seg_length_ft':'length',
            'seg_width_ft':'width',
            'moratorium':'date_moratorium',
            'start':'date_start',
            'end':'date_end'
            })

        final_cols = ['pve_id','seg_id','project_id','title','project_manager',
        'project_manager_phone','status','type','resident_engineer','address_street',
        'street_from','street_to','length','width','date_moratorium',
        'date_start','date_end','paving_miles']

        df_final = df[final_cols].copy()

        df_final['status'] = df_final['status'].str.lower()

    
    # Write csv
    logging.info('Writing ' + str(df_final.shape[0]) + ' rows in mode ' + mode)
    general.pos_write_csv(
        df_final, prod_file[mode], date_format=conf['date_format_ymd'])
    
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
