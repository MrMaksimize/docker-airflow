import pandas as pd
import geopandas as gpd
import math
import fiona
from fiona import crs
from shapely.geometry import mapping
from shapely.geometry import LineString
import requests
import numpy as np
from datetime import datetime, timedelta
import logging
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general
from trident.util.geospatial import shp2zip
from collections import OrderedDict
from arcgis import GIS
from arcgis.features import FeatureLayerCollection

conf = general.config

temp_query = conf['temp_data_dir'] + '/cartegraph_results.csv'
temp_file = conf['temp_data_dir'] + '/sd_paving_base.csv'
temp_gis = conf['temp_data_dir'] + '/sd_paving_esri_base.csv'

prod_file = {
    'sdif': conf['prod_data_dir'] + '/sd_paving_datasd_v1.csv',
    'imcat': conf['prod_data_dir'] + '/sd_paving_imcat_datasd_v1.csv'
}

esri_layer = {
    'completed': {
        'selection':['post construction'],
        'feature_lyr':'46b86c7c13f342f28a7636e1763633dd'},
    'in_progress': {
        'selection':['construction'],
        'feature_lyr':'36523bce316c41ab88f555d99d1130bf'},
    'planned': {
        'selection':['planning','design','bid / award'],
        'feature_lyr':'0a3b0632f980495fb7946b68040b3732'},
}

#: Helper function
def number_str_cols(col):
    col = col.fillna(-9999.0)
    col = col.astype(int)
    col = col.astype(str)
    col = col.replace('-9999', '')

    return col

#: Helper function
def get_paving_miles(row):
    """ Calculate paving miles """
    
    if row['seg_width_ft'] >= 50:
        return (row['seg_length_ft'] * 2)/5280
    else:
        return row['seg_length_ft']/5280

#: Helper function
def get_start_end_dates(row):
    """ Determine correct start and end dates """

    if row['wo_id'] == 'UTLY' or row['wo_id'] == 'TSW':
        return row['job_start_dt'], row['job_end_dt']

    else:

        if row['job_completed_cbox'] == 1:
            return row['job_end_dt'], row['job_end_dt']

        else:
            return row['start'], row['end']

#: DAG function
def get_streets_paving_data():
    """Get streets paving data from DB."""
    
    pv_query = general.file_to_string('./sql/pavement_ex.sql', __file__)
    pv_conn = MsSqlHook(mssql_conn_id='streets_cg_sql')

    df = pv_conn.get_pandas_df(pv_query)

    results = df.shape[0]

    general.pos_write_csv(
        df, temp_query)
    
    return f"Successfully wrote temp file with {results} records"

#: DAG function
def create_base_data():
    """ Process paving data with changes for both modes """
    
    moratorium_string = "Post Construction"
    phone_UTLY = "858-627-3200"
    phone_OTHER = "619-527-7500"
    TSW_PM = "JLahmann@sandiego.gov"
    UTLY_PM = "Engineering@sandiego.gov"
    ACT_OVERLAY_CONCRETE_PM = "CHudson@sandiego.gov"
    ACT_SLURRY_SERIES_PM = "AVance@sandiego.gov"
    ACT_SERIES_CIRCUIT_PM = "CHoenes@sandiego.gov"

    today = general.today()

    date_cols = ['wo_design_start_dt',
    'wo_design_end_dt',
    'job_start_dt',
    'job_end_dt']

    df = pd.read_csv(temp_query,low_memory=False,
        parse_dates=date_cols)

    # Update column types

    float_cols = ['pve_id',
    'rd_seg_id',
    'seg_council_district']

    for i in float_cols:
        df[i] = number_str_cols(df[i])

    str_cols = ['seg_id',
    'wo_proj_type',
    'wo_id',
    'wo_name']

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

    logging.info('Rounding length in feet and calculating paving miles')

    #First, round segment length to next whole number
    df['seg_length_ft'] = df['seg_length_ft'].fillna(0.0)
    df['seg_length_ft'] = df['seg_length_ft'].apply(lambda x: math.ceil(x))

    
    paving_miles = df.apply(get_paving_miles, axis=1)
    df = df.assign(paving_miles=paving_miles)

    # Now we need to set paving miles to zero for certain activity type
    df.loc[df['job_activity'] == 'AC - Surface Treatment Partial','paving_miles'] = 0

    #*** Remove unneeded records ***
    # Additional record removal based on mode happens in next task

    logging.info(f"Starting with {df.shape[0]} rows before removing records")

    start_no = df.shape[0]

    # UTLY jobs where job end date is missing
    df = df[~((df.wo_id == "UTLY") & (df.job_end_dt.isnull()))]
    logging.info(f"Removed {start_no - df.shape[0]} UTLY records with missing job end")
    start_no = df.shape[0]

    # Records for data entry, mill / pave, structure widening, and patching
    remove_search = 'data entry|mill|structure wid|patching'
    df = df[~(df.job_activity.str.contains(
        remove_search, regex=True, case=False, na=False))]
    logging.info(f"Removed {start_no - df.shape[0]} records for data entry, etc")
    start_no = df.shape[0] 

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

    # Write csv
    logging.info('Writing base data')
    general.pos_write_csv(
        df, temp_file, date_format=conf['date_format_ymd'])
    
    return "Successfully wrote base file"

#: DAG function
def create_mode_data(mode='sdif', **context):
    """ Creating final files based on mode """

    date_cols = ['wo_design_start_dt',
    'wo_design_end_dt',
    'job_start_dt',
    'job_end_dt']

    df = pd.read_csv(temp_file,low_memory=False,parse_dates=date_cols)

    exec_date = context['execution_date'].in_tz(tz='US/Pacific')

    five_yrs_ago = exec_date.subtract(years=5).strftime('%Y-%m-%d')
    three_yrs_ago = exec_date.subtract(years=3).strftime('%Y-%m-%d')

    if mode == 'imcat':

        # All older than 5 years plus slurry records older than 3 years for imcat
        start_no = df.shape[0]
        logging.info(f"Starting with {start_no} records")

        df = df[(df['job_end_dt'] > five_yrs_ago) | (df['job_end_dt'].isnull())]
        logging.info(f"Removed {start_no - df.shape[0]} records older than 5 years")
        df = df[~((df.wo_proj_type == 'Slurry') &
                 (df['job_end_dt'] < three_yrs_ago))]
        logging.info(f"Removed {start_no - df.shape[0]} Slurry records older than 3 years")

        # Remove duplicates for imcat. Must be unique project list

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

    elif mode == 'sdif':

        # Keep duplicates and anything after July 1 2013 for public dataset
        start_no = df.shape[0]
        logging.info(f"Starting with {start_no} records")

        df = df[(df.job_end_dt >= '07/01/2013') | (df.job_end_dt.isnull())]
        logging.info(f"Removed {start_no - df.shape[0]} records older than July 1, 2013")

        df = df.sort_values(by=['seg_id','job_end_dt'], na_position='last', ascending=[True,False])

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


#: DAG function
def create_arcgis_base():
    """ Create GIS file and send to ArcGIS online """
    
    logging.info("Reading repair data")
    df = pd.read_csv(prod_file['sdif'],
        low_memory=False,
        parse_dates=['date_end','date_start'])

    logging.info("Creating calendar and fiscal year cols")

    df['date_cy'] = df['date_end'].apply(lambda x: x.strftime('%Y') if not pd.isnull(x) else '')
    df['date_fy'] = df['date_end'].apply(lambda x: str(x.year+1) if x.month > 6 else str(x.year) if not pd.isnull(x) else '' )
    df['date_start'] = df['date_start'].apply(lambda x: x.strftime('%m/%Y') if not pd.isnull(x) else '')
    df['date_end'] = df['date_end'].apply(lambda x: x.strftime('%m/%Y') if not pd.isnull(x) else '')

    logging.info("Renaming cols to meet character limits")
    
    df = df.rename(columns={'address_street':'addr_st',
        'street_from':'street_fr',
        'street_to':'street_to',
        'paving_miles':'pav_mi'})

    logging.info("Creating miles completed for chart")

    df.loc[df['status'] == 'post construction','mi_comp'] = df.loc[df['status'] == 'post construction','pav_mi']
    df.loc[df['status'] != 'post construction','mi_comp'] = 0

    logging.info("Creating subset for merging")

    df_sub = df[['pve_id',
    'seg_id',
    'project_id',
    'title',
    'status',
    'type',
    'date_start',
    'date_end',
    'pav_mi',
    'date_cy',
    'date_fy',
    'mi_comp']]

    logging.info("Filling in NAs")

    df_sub['type'] = df_sub['type'].fillna('None')

    na_cols = ['pve_id','seg_id','project_id','title','status']
    for nc in na_cols:
        df_sub[nc] = df_sub[nc].fillna('')


    logging.info("Reading in OCI data")

    oci_11 = pd.read_csv(f"{conf['prod_data_dir']}/oci_2011_datasd.csv")
    oci_15 = pd.read_csv(f"{conf['prod_data_dir']}/oci_2015_datasd.csv")

    logging.info("Merging segment OCI to create OCI cols")

    merge_oci = pd.merge(df_sub,oci_11[['seg_id','oci','oci_desc']],how='left',on='seg_id')
    merge_oci = merge_oci.rename(columns={'oci':'oci_11','oci_desc':'oci11_des'})
    
    final_pave_gis = pd.merge(merge_oci,oci_15[['seg_id','oci','oci_desc']],how='left',on='seg_id')
    final_pave_gis = final_pave_gis.rename(columns={'oci':'oci_15','oci_desc':'oci15_des'})

    logging.info("Writing data for layer creation")

    general.pos_write_csv(
        final_pave_gis,
        temp_gis,
        date_format=conf['date_format_ymd'])

    return "Successfully created GIS base for ESRI"

def send_arcgis(mode=['completed'], **context):
    """ Update ArcGIS online feature layer """

    logging.info("Read in ESRI base file")

    df = pd.read_csv(temp_gis,
        low_memory=False,
        dtype={'date_cy':str,
        'date_fy':str,
        'date_start':str,
        'date_end':str,
        'pve_id':str})

    logging.info(f"Divide {df.shape[0]} rows of data into layers")

    layer = df.loc[df['status'].isin(esri_layer[mode].get('selection')),:]
    logging.info(f"{mode} layer has {layer.shape[0]} rows")

    if not layer.empty:

        logging.info("Reading in line segments")

        geojson = gpd.read_file(f"{conf['prod_data_dir']}/sd_paving_segs_datasd.geojson")

        geojson = geojson.to_crs("EPSG:2230")

        logging.info("Merging paving data with street segments")

        layer_merge = pd.merge(layer,
            geojson,
            how='left',
            left_on='seg_id',
            right_on='sapid')

        logging.info("Set sapid equal to segid when null")

        layer_merge = layer_merge.drop(columns={'seg_id'})
        layer_merge = layer_merge.rename(columns={'sapid':'seg_id'})
        
    
        logging.info("Writing layer to shapefile")
        layer_name = f'sd_paving_gis_{mode}'

        final = gpd.GeoDataFrame(layer_merge,geometry='geometry',crs="EPSG:2230")

        final = final[['roadsegid',
        'rd20full',
        'xstrt1',
        'xstrt2',
        'llowaddr',
        'lhighaddr',
        'rlowaddr',
        'rhighaddr',
        'zip',
        'pve_id',
        'seg_id',
        'project_id',
        'title',
        'status',
        'type',
        'date_start',
        'date_end',
        'pav_mi',
        'mi_comp',
        'date_cy',
        'date_fy',
        'oci_11',
        'oci11_des',
        'oci_15',
        'oci15_des',
        'geometry']]

        shp_path = f"{conf['prod_data_dir']}/{layer_name}"

        # Write to csv for QA


        final.to_file(f"{shp_path}.shp")

        shp2zip(layer_name)

        arc_gis = GIS("https://SanDiego.maps.arcgis.com",conf["arc_online_user"],conf["arc_online_pass"])
        # This depends on mode
        lyr_id = esri_layer[mode].get('feature_lyr')
        shape_file = arc_gis.content.get(lyr_id)
        streets_flayer_collection = FeatureLayerCollection.fromitem(shape_file)
        logging.info("Overwriting streets feature layer collection")
        overwrite = streets_flayer_collection.manager.overwrite(f"{shp_path}.zip")

        return overwrite

    else:

        return f"{mode} could not update because the selection is empty"

def check_exec_time(**context):
    currTime = context['execution_date'].in_timezone('America/Los_Angeles')
    if currTime.hour == 16:
        if currTime.day_of_week <= 4 :
            logging.info(f'Calling downstream tasks, hour is: {currTime.hour}')
            return True
        else:
            logging.info(f'Skipping downstream tasks, day is: {currTime.day_of_week}')
            return False
    else:
        logging.info(f'Skipping downstream tasks, hour is: {currTime.hour}')
        return False
    