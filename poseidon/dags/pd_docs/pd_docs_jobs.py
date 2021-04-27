""" PD docs _jobs file """

# Required imports

from trident.util import general
import logging

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

import boto3

# -- Imports for transformations

import pandas as pd

# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# Generic functions called in template dags

def get_themis_contents():
    """ 
    This is a function for the basic Python Operator
    """
    
    conn = boto3.client('s3')
    themis_contents = conn.list_objects(Bucket='themis2.datasd.org')

    data = {}
    prefix = ''

    for s3_object in themis_contents['Contents']: 
        
        ### Identify if object is a folder (size = 0 bytes) and skip
        if s3_object['Size'] == 0:
            continue
            
        ### Build S3 download URL
        temp_key = s3_object['Key']
        temp_name = temp_key.replace(prefix,'')
        temp_obj_url = (s3_object['Key']).replace(' ','+')
        temp_url = 'https://s3-us-west-2.amazonaws.com/themis2.datasd.org/'+temp_obj_url
        
        ### Remove labratory folder
        if temp_key[0:3] == 'lab':
            continue
        
        ### Identify modified date
        temp_mod_date = s3_object['LastModified']
        
        ### Build category string
        prefix = s3_object['Key']
        category_string = prefix.replace('policies_procedures/','')
        subcategory_list = category_string.split('/')
        category = subcategory_list[0]
        
        ### Build subcategory string
        subcategory = '-'.join(subcategory_list)
        subcategory = subcategory.replace(category,'')
        subcategory = subcategory[1:]
        
        ### Create final dictionary, to be passed into pandas dataframe
        data[temp_key] = [temp_name, category, subcategory, temp_url, temp_mod_date]

    ### Create pandas dataframe from data dictionary and write to csv
    pd_policy_df = pd.DataFrame.from_dict(data,
        orient='index',
        columns=['Title', 
        'Category', 
        'Subcategory', 
        'Document_URL', 
        'Modify_Date'])

    final_df = pd_policy_df.loc[pd_policy_df['Category'] != 'index.html']
    
    general.pos_write_csv(
        final_df,
        f"{conf['prod_data_dir']}/pd_policy.csv",
        date_format="%Y-%m-%d %H:%M:%S")

    return "Successfully generated file of PD docs"