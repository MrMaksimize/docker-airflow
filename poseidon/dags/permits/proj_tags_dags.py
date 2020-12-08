"""DSD Permits subdags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.permits.proj_tags_jobs import *
from trident.util.seaboard_updates import *
from trident.util.snowflake_client import *

conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

snowflake_stage = format_stage_sql('dsd_proj_tags')
snowflake_del = format_delete_sql('dsd_proj_tags')
snowflake_copy = format_copy_sql('dsd_proj_tags')

dag = DAG(
  dag_id='dsd_proj_tags',
  default_args=args,
  start_date=start_date,
  schedule_interval=schedule,
  catchup=False
)

get_file = PythonOperator(
  task_id='get_tags_files',
  provide_context=True,
  python_callable=get_tags_file,
  dag=dag)

create_prod = PythonOperator(
  task_id="create_tags_prod",
  provide_context=True,
  python_callable=build_tags,
  dag=dag)

upload_file = S3FileTransferOperator(
  task_id="upload_tags",
  source_base_path=conf['prod_data_dir'],
  source_key="permits_set1_project_tags_datasd.csv",
  dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
  dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
  dest_s3_key="dsd/permits_set1_project_tags_datasd.csv",
  replace=True,
  dag=dag)

update_tags_md = get_seaboard_update_dag('development-permits-tags.md', dag)

#: Update data inventory json
update_json_date = PythonOperator(
  task_id='update_json_date',
  python_callable=update_json_date,
  provide_context=True,
  op_kwargs={'ds_fname': 'development-permits-tags'},
  dag=dag)

stage_snowflake = SnowflakeOperator(
  task_id=f"stage_snowflake_dsd_proj_tags",
  sql=snowflake_stage,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

delete_snowflake = SnowflakeOperator(
  task_id=f"del_snowflake_dsd_proj_tags",
  sql=snowflake_del,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

copy_snowflake = SnowflakeOperator(
  task_id=f"copy_snowflake_dsd_proj_tags",
  sql=snowflake_copy,
  snowflake_conn_id="snowflake",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

#: Execution rules
get_file>>create_prod>>upload_file>>[update_tags_md,update_json_date]
create_prod>>stage_snowflake>>delete_snowflake>>copy_snowflake