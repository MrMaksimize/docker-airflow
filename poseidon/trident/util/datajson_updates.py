"""Utilities for Updating data.json catalog"""
import re, base64, os
import logging
import random
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from trident.util import general
from trident.util.notifications import notify
import json


conf = general.config

def update_json_date(ds_fname, **kwargs):
    
    exec_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    test_mode = kwargs['test_mode']

    logging.info(f"Looking for {ds_fname}")

    json_path = f"{conf['prod_data_dir']}/data.json"

    with open(json_path) as json_file:
        data = json.load(json_file)

    # if test_mode is not True:

    for dataset in data['dataset']:
        if ds_fname == dataset['identifier']:
            prev_date = dataset['modified']
            if prev_date != exec_date:
                dataset['modified'] = exec_date
                # Need stuff in here to upload to Netlify
            else:
                return f"{ds_fname} already matches {exec_date}"

    with open (json_path, 'w') as outfile:
        json.dump(data, outfile, indent=4)

    return "Successfully checked date updated"

def update_datajson_dag(ds_fname, dag):
    task = PythonOperator(
        task_id=f"update_{ds_fname}",
        python_callable=update_json_date,
        provide_context=True,
        op_kwargs={'ds_fname': ds_fname},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    return task
