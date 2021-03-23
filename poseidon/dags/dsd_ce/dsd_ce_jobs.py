""" Code Enforcement Job """

# Required imports

from trident.util import general
import logging

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
    
    logging.info("Running Python operator task with context")

    # Get context
    exec_date = context['execution_date']

    logging.info(f"Run date is {exec_date}")

    # Push context
    context['task_instance'].xcom_push(key='temp_check', value=True)

    return "Successfully completed context function"
