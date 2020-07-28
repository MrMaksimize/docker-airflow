"""DSD Permits subdags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.permits.permits_jobs import *
from trident.util.snowflake_client import *


conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

files = ['set1_active',
'set1_closed',
'set1_closed_projects',
'set2_active',
'set2_closed']

snowflake_files = ['dsd_approvals_pts','dsd_approvals_accela']

def create_file_subdag():
  """
  Generate a DAG to be used as a subdag 
  that creates permit files 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.create_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  for file in files:
    mode = file.split('_')[-1]
    sys = file.split('_')[0]

    if sys == 'set1': 

      create_file = PythonOperator(
        task_id=f"create_{file}",
        provide_context=True,
        python_callable=build_pts,
        op_kwargs={'mode': mode},
        dag=dag_subdag)

    else:

      create_file = PythonOperator(
        task_id=f"create_{file}",
        provide_context=True,
        python_callable=build_accela,
        op_kwargs={'mode': mode},
        dag=dag_subdag)

  return dag_subdag


def join_bids_subdag():
  """
  Generate a DAG to be used as a subdag 
  that joins BIDs to permit files 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.join_bids',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  for file in files:

    join_bids = PythonOperator(
        task_id=f"join_bids_{file}",
        provide_context=True,
        python_callable=join_bids_permits,
        op_kwargs={'pt_file': file},
        dag=dag_subdag)

  return dag_subdag

def upload_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that joins BIDs to permit files 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.upload_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  for file in files:

    upload_file = S3FileTransferOperator(
      task_id=f"upload_{file}",
      source_base_path=conf['prod_data_dir'],
      source_key=f"permits_{file}_datasd.csv",
      dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
      dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
      dest_s3_key=f"dsd/permits_{file}_datasd.csv",
      replace=True,
      dag=dag_subdag)

  upload_file = S3FileTransferOperator(
    task_id="upload_pts_all",
    source_base_path=conf['prod_data_dir'],
    source_key="dsd_permits_all_pts.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key="dsd/dsd_permits_all_pts.csv",
    replace=True,
    dag=dag_subdag,
    )

  upload_file = S3FileTransferOperator(
    task_id="upload_accela_all",
    source_base_path=conf['prod_data_dir'],
    source_key="dsd_permits_all_accela.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key="dsd/dsd_permits_all_accela.csv",
    replace=True,
    dag=dag_subdag,
    )

  return dag_subdag

def snowflake_subdag():
  """
  Generate a DAG to be used as a subdag
  that updates tables into Snowflake
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.snowflake',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for file in snowflake_files:

    snowflake_stage = format_stage_sql(file)
    snowflake_del = format_delete_sql(file)
    snowflake_copy = format_copy_sql(file)

    stage_snowflake = SnowflakeOperator(
      task_id=f"stage_snowflake_{file}",
      sql=snowflake_stage,
      snowflake_conn_id="snowflake",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)
    
    delete_snowflake = SnowflakeOperator(
      task_id=f"del_snowflake_{file}",
      sql=snowflake_del,
      snowflake_conn_id="snowflake",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)
  
    copy_snowflake = SnowflakeOperator(
      task_id=f"copy_snowflake_{file}",
      sql=snowflake_copy,
      snowflake_conn_id="snowflake",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)

  return dag_subdag