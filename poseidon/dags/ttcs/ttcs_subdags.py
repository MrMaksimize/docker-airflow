"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general

# Optional import if uploading within subdag
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator


from dags.ttcs.ttcs_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['ttcs']
start_date = general.start_date['ttcs']

queries = ['main','location','dba','phone','email']

def create_subdag_operators():
  """
  Generate a DAG to be used as a subdag 
  that updates ESRI map layers
  """

  dag_subdag = DAG(
    dag_id='ttcs.query_ttcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  ops = []

  for query in queries:

    #: Get active businesses and save as .csv to temp folder
    execute_query = PythonOperator(
        task_id=f'query_{query}',
        python_callable=query_ttcs,
        provide_context=True,
        op_kwargs={'mode': query},
        dag=dag_subdag)

    ops.append(execute_query)

  ops[0] >> ops[1] >> ops[2] >> ops[3] >> ops[4]

  return dag_subdag

def create_upload_operators():
  """
  Generate a DAG to be used as a subdag 
  that updates ESRI map layers
  """

  dag_subdag = DAG(
    dag_id='ttcs.upload_ttcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  active = S3FileTransferOperator(
    task_id='upload_active',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_active_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_active_datasd.csv',
    replace=True,
    dag=dag_subdag)

  shop_local = S3FileTransferOperator(
    task_id='upload_shop_local',
    source_base_path=conf['prod_data_dir'],
    source_key='shop_local_businesses.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/shop_local_businesses.csv',
    replace=True,
    dag=dag_subdag)

  inactive_hist = S3FileTransferOperator(
    task_id='upload_inactive_hist',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_pre1990_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_pre1990_datasd.csv',
    replace=True,
    dag=dag_subdag)

  inactive_90 = S3FileTransferOperator(
    task_id='upload_inactive_1990',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_1990to2000_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_1990to2000_datasd.csv',
    replace=True,
    dag=dag_subdag)

  inactive_00 = S3FileTransferOperator(
    task_id='upload_inactive_2000',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_2000to2010_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_2000to2010_datasd.csv',
    replace=True,
    dag=dag_subdag)

  inactive_10 = S3FileTransferOperator(
    task_id='upload_inactive_2010',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_2010to2015_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_2000to2010_datasd.csv',
    replace=True,
    dag=dag_subdag)

  inactive_current = S3FileTransferOperator(
    task_id='upload_inactive_current',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_businesses_inactive_2015tocurr_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='ttcs/sd_businesses_inactive_2015tocurr_datasd.csv',
    replace=True,
    dag=dag_subdag)

  return dag_subdag