""" Code Enforcement Job """

# Required imports

from trident.util import general
import logging
from airflow.hooks.base_hook import BaseHook

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something



# -- Imports for transformations

import pandas as pd


# Optional variables

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# Generic functions called in template dags

def query_data(**context):
    """ 
    This is a function for a Python Operator with context
    """
    
    logging.info("connecting to Azure")
    conn = BaseHook.get_connection(conn_id="DSD_ACCELA")

    logging.info(conn.host)

    # Get context
    exec_date = context['execution_date']

    logging.info(f"Run date is {exec_date}")

    

    return "Successfully completed context function"
