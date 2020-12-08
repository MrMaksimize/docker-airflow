"""Get It Done _dags file."""
import re
import glob
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

from trident.util import general
from trident.util.snowflake_client import *
from trident.util.notifications import afsys_send_email
from trident.util.seaboard_updates import *

from dags.get_it_done.gid_jobs import *
from dags.get_it_done.gid_subdags import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['get_it_done']
start_date = general.start_date['get_it_done']

snowflake_stage = format_stage_sql('get_it_done')
snowflake_del = format_delete_sql('get_it_done')
snowflake_copy = format_copy_sql('get_it_done')

#: Dag spec
dag = DAG(dag_id='get_it_done',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Get GID requests from Salesforce
get_streets_requests = PythonOperator(
    task_id='get_gid_streets',
    python_callable=get_gid_streets,
    dag=dag)

#: Get GID requests from Salesforce
get_other_requests = PythonOperator(
    task_id='get_gid_other',
    python_callable=get_gid_other,
    dag=dag)

#: Create mapped case record type and service name cols
update_service_name = PythonOperator(
    task_id='update_service_name',
    python_callable=update_service_name,
    dag=dag)

#: Fix close dates per SAP issue
update_close_dates = PythonOperator(
    task_id='update_close_dates',
    python_callable=update_close_dates,
    dag=dag)

#: Fix referral column
update_referral_col = PythonOperator(
    task_id='update_referral_col',
    python_callable=update_referral_col,
    dag=dag)

#: Fix referral column
create_stormwater_gis = PythonOperator(
    task_id='stormwater_gis',
    python_callable=create_stormwater_gis,
    dag=dag)

upload_stormwater_gis = S3FileTransferOperator(
    task_id=f'upload_sw_gis',
    source_base_path=conf['prod_data_dir'],
    source_key=f'discharges_abated.geojson',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'get_it_done_311/discharges_abated.geojson',
    replace=True,
    dag=dag)

upload_stormwater_csv = S3FileTransferOperator(
    task_id=f'upload_sw_csv',
    source_base_path=conf['prod_data_dir'],
    source_key=f'discharges_abated.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'get_it_done_311/discharges_abated.csv',
    replace=True,
    dag=dag)

spatial_joins = SubDagOperator(
  task_id='spatial_joins',
  subdag=spatial_join_subdag(),
  dag=dag)

service_names = SubDagOperator(
  task_id='service_names',
  subdag=service_name_subdag(),
  dag=dag)

#: Divide records by year for prod files
create_prod_files = PythonOperator(
    task_id='create_prod_files',
    python_callable=create_prod_files,
    dag=dag)

stage_snowflake = SnowflakeOperator(
  task_id="stage_snowflake",
  sql=snowflake_stage,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

# IMPORTANT - only use this if the table needs to be
# deleted before adding new data
delete_snowflake = SnowflakeOperator(
  task_id="del_snowflake",
  sql=snowflake_del,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

copy_snowflake = SnowflakeOperator(
  task_id="copy_snowflake",
  sql=snowflake_copy,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

upload_prod_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(),
  dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'get_it_done_reports'},
    dag=dag)

md_update_task = get_seaboard_update_dag('get-it-done-311.md', dag)
            
#: Execution rules
[get_streets_requests, get_other_requests] >> update_service_name
update_service_name >> update_close_dates
update_close_dates >> update_referral_col
update_referral_col >> spatial_joins
update_referral_col >> create_stormwater_gis >> [upload_stormwater_gis,upload_stormwater_csv]
spatial_joins >> create_prod_files >> service_names >> upload_prod_files
create_prod_files >> stage_snowflake >> delete_snowflake >> copy_snowflake
upload_prod_files >> [md_update_task,update_json_date]
