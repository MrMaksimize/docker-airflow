"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.ttcs.ttcs_jobs import *
from trident.util.seaboard_updates import *
from dags.ttcs.ttcs_subdags import *

import os
import glob

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['ttcs']


#: Dag definition
dag = DAG(dag_id='ttcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['ttcs'],
    catchup=False
    )

# Execute queries
query_subdag = SubDagOperator(
    task_id='query_ttcs',
    subdag=create_subdag_operators(),
    dag=dag,
  )

#: Process temp data and save as .csv to prod folder
clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag)

#: Geocode new entries and update production file
geocode_data = PythonOperator(
    task_id='geocode_data',
    python_callable=geocode_data,
    dag=dag)

addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_address_book',
    source_base_path=conf['prod_data_dir'],
    source_key='ttcs_address_book.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/ttcs_address_book.csv',
    replace=True,
    dag=dag)

#: Create subsets
create_subsets = PythonOperator(
    task_id='create_subsets',
    python_callable=make_prod_files,
    dag=dag)

#: Get active businesses and save as .csv to temp folder
execute_query = PythonOperator(
    task_id=f'query_pins',
    python_callable=query_ttcs,
    provide_context=True,
    op_kwargs={'mode': 'pins'},
    dag=dag)

# Execute queries
upload_subdag = SubDagOperator(
    task_id='upload_ttcs',
    subdag=create_upload_operators(),
    dag=dag)

#: Update portal modified date
update_ttcs_md = get_seaboard_update_dag('business-listings.md', dag)

#: Execution Rules
query_subdag >> clean_data >> geocode_data >> addresses_to_S3
addresses_to_S3 >> create_subsets >> upload_subdag >> update_ttcs_md
