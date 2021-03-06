"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.ttcs.ttcs_jobs import *
from trident.util.seaboard_updates import *
from dags.ttcs.ttcs_subdags import *
from trident.util.snowflake_client import *

import os
import glob

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['ttcs']
snowflake_stage = format_stage_sql('tax_certs')
snowflake_del = format_delete_sql('tax_certs')
snowflake_copy = format_copy_sql('tax_certs')


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
query_pins = PythonOperator(
    task_id=f'query_pins',
    python_callable=query_ttcs,
    provide_context=True,
    op_kwargs={'mode': 'pins'},
    dag=dag)

upload_pins = S3FileTransferOperator(
    task_id='upload_pins',
    source_base_path=conf['prod_data_dir'],
    source_key='ttcs-pins.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_INTERNAL_BUCKET }}",
    dest_s3_key='gis/ttcs-pins.csv',
    replace=True,
    dag=dag)

#: Upload shop local to arcgis
upload_arcgis = PythonOperator(
    task_id=f'send_arcgis',
    python_callable=send_arcgis,
    dag=dag)

# Upload files
upload_subdag = SubDagOperator(
    task_id='upload_ttcs',
    subdag=create_upload_operators(),
    dag=dag)

stage_snowflake = SnowflakeOperator(
  task_id="stage_snowflake",
  sql=snowflake_stage,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="businesses",
  schema="public",
  dag=dag)

delete_snowflake = SnowflakeOperator(
  task_id="del_snowflake",
  sql=snowflake_del,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="businesses",
  schema="public",
  dag=dag)

copy_snowflake = SnowflakeOperator(
  task_id="copy_snowflake",
  sql=snowflake_copy,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="businesses",
  schema="public",
  dag=dag)

#: Update portal modified date
update_ttcs_md = get_seaboard_update_dag('business-listings.md', dag)

#: Execution Rules
query_subdag >> clean_data >> geocode_data >> addresses_to_S3
addresses_to_S3 >> create_subsets >> upload_subdag >> update_ttcs_md
create_subsets >> stage_snowflake >> delete_snowflake >> copy_snowflake
query_pins >> upload_pins
query_pins >> upload_arcgis
create_subsets >> upload_arcgis
