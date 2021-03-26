from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import DAG
from trident.util import general

from dags.nextrequest.nr_jobs import *
from datetime import datetime
from trident.util.snowflake_client import *

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = '@daily'
start_date = datetime(2021, 3, 25)
conf = general.config

dag = DAG(
    dag_id='nextrequest',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Downloads nextrequest data from API
request_pra_date = PythonOperator(
    task_id='request_pra_date',
    python_callable=request_pra_date,
    provide_context=True,
    dag=dag)

#: Joins downloaded files from API to production
update_prod = PythonOperator(
    task_id='update_prod',
    python_callable=update_prod,
    provide_context=True,
    dag=dag)


#: Uploads (S3) the pv production file
s3_upload = S3FileTransferOperator( # creating a different upload object for each...
    task_id='s3_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='nextrequest_prod.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'nextrequest/nextrequest_prod.csv',
    replace=True,
    dag=dag)

snowflake_stage = format_stage_sql('nextrequest')
snowflake_del = format_delete_sql('nextrequest')
snowflake_copy = format_copy_sql('nextrequest')

#: Snowflake upload
stage_snowflake_nr = SnowflakeOperator(
  task_id=f"stage_snowflake_nr",
  sql=snowflake_stage,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

delete_snowflake_nr = SnowflakeOperator(
  task_id=f"delete_snowflake_nr",
  sql=snowflake_del,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

copy_snowflake_nr = SnowflakeOperator(
  task_id=f"copy_snowflake_nr",
  sql=snowflake_copy,
  snowflake_conn_id="SNOWFLAKE",
  warehouse="etl_load",
  database="open_data",
  schema="public",
  dag=dag)

request_pra_date >> update_prod >> s3_upload >> stage_snowflake_nr >> delete_snowflake_nr >> copy_snowflake_nr