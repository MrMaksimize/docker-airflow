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

snowflake_files = ['dsd_approvals_pts','dsd_approvals_accela']

def get_create_accela_subdag():
  """
  Generate a DAG to be used as a subdag
  for Accela tasks
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.get_create_accela',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  #: Get permits reports
  get_files = PythonOperator(
    task_id='get_accela_files',
    provide_context=True,
    python_callable=get_permits_files,
    op_kwargs={'mode': 'accela'},
    dag=dag_subdag)

  create = PythonOperator(
    task_id=f"create_set2",
    provide_context=True,
    python_callable=build_accela,
    dag=dag_subdag)

  join = PythonOperator(
    task_id=f"join_accela",
    python_callable=spatial_joins,
    op_kwargs={'pt_file': 'dsd_permits_all_accela'},
    dag=dag_subdag)

  subset = PythonOperator(
    task_id=f"subset_accela",
    provide_context=True,
    python_callable=create_subsets,
    op_kwargs={'mode': 'set2'},
    dag=dag_subdag)

  get_files>>create>>join>>subset

  return dag_subdag

def get_create_pts_subdag():
  """
  Generate a DAG to be used as a subdag
  for PTS tasks 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.get_create_pts',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  #: Get permits reports
  get_files = PythonOperator(
    task_id='get_pts_files',
    provide_context=True,
    python_callable=get_permits_files,
    op_kwargs={'mode': 'pts'},
    dag=dag_subdag)

  create = PythonOperator(
    task_id=f"create_set1",
    provide_context=True,
    python_callable=build_pts,
    dag=dag_subdag)

  join = PythonOperator(
    task_id=f"join_pts",
    python_callable=spatial_joins,
    op_kwargs={'pt_file': 'dsd_permits_all_pts'},
    dag=dag_subdag)

  subset = PythonOperator(
    task_id=f"subset_pts",
    provide_context=True,
    python_callable=create_subsets,
    op_kwargs={'mode': 'set1'},
    dag=dag_subdag)

  get_files>>create>>join>>subset

  return dag_subdag

def upload_set1_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that joins BIDs to permit files 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.upload_set1_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  files = ['set1_active',
  'set1_closed']

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

  upload_pts = S3FileTransferOperator(
    task_id="upload_pts_all",
    source_base_path=conf['prod_data_dir'],
    source_key="dsd_permits_all_pts.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key="dsd/dsd_permits_all_pts.csv",
    replace=True,
    dag=dag_subdag,
    )

  upload_pts_polygons = S3FileTransferOperator(
    task_id='upload_pts_polygons',
    source_base_path=conf['prod_data_dir'],
    source_key='dsd_permits_all_pts_polygons.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/dsd_permits_all_pts_polygons.csv',
    replace=True,
    dag=dag_subdag)

  return dag_subdag

def upload_set2_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that joins BIDs to permit files 
  """

  dag_subdag = DAG(
    dag_id='dsd_permits.upload_set2_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

  files = ['set2_active',
  'set2_closed']

  for file in files:

    upload_file = S3FileTransferOperator(
      task_id=f"upload_{file}",
      source_base_path=conf['prod_data_dir'],
      source_key=f"permits_{file}_datasd.csv",
      dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
      dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
      dest_s3_key=f"dsd/permits_{file}_datasd.csv",
      replace=True,
      dag=dag_subdag
      )

  upload_accela = S3FileTransferOperator(
    task_id="upload_accela_all",
    source_base_path=conf['prod_data_dir'],
    source_key="dsd_permits_all_accela.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key="dsd/dsd_permits_all_accela.csv",
    replace=True,
    dag=dag_subdag,
    )

  upload_accela_polygons = S3FileTransferOperator(
    task_id='upload_accela_polygons',
    source_base_path=conf['prod_data_dir'],
    source_key='dsd_permits_all_accela_polygons.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/dsd_permits_all_accela_polygons.csv',
    replace=True,
    dag=dag_subdag)

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
      snowflake_conn_id="SNOWFLAKE",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)
    
    delete_snowflake = SnowflakeOperator(
      task_id=f"del_snowflake_{file}",
      sql=snowflake_del,
      snowflake_conn_id="SNOWFLAKE",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)
  
    copy_snowflake = SnowflakeOperator(
      task_id=f"copy_snowflake_{file}",
      sql=snowflake_copy,
      snowflake_conn_id="SNOWFLAKE",
      warehouse="etl_load",
      database="open_data",
      schema="public",
      dag=dag_subdag)

    stage_snowflake>>delete_snowflake>>copy_snowflake

  return dag_subdag