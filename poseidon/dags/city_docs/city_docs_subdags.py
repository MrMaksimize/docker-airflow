""" Documentum web tables with one hour schedule update _dags file"""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify
import dags.city_docs.documentum_name as dn

from dags.city_docs.city_docs_jobs import *
from datetime import datetime

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule
start = general.start_date
parent_sched = {
'documentum_hourly_30': schedule.get('documentum_hr_30'),
'documentum_daily': schedule.get('documentum_daily'),
'documentum_hourly_15': schedule.get('documentum_hr_15'),
'documentum_test': start.get('documentum_hr_30')
}
parent_start = {
'documentum_hourly_30': start.get('documentum_hr_30'),
'documentum_daily': start.get('documentum_daily'),
'documentum_hourly_15': start.get('documentum_hr_15'),
'documentum_test': start.get('documentum_hr_30')
}

def upload_files_subdag(files,parent_dag,test):
  """
  Generate a DAG to be used as a subdag 
  that uploads city docs files 
  """

  conf_name = schedule.get(parent_dag)

  dag_subdag = DAG(
    dag_id=f'{parent_dag}.upload_files',
    default_args=args,
    start_date=parent_start[parent_dag],
    schedule_interval=parent_sched[parent_dag],
    catchup=False
  )

  for file in files:

    file_low = file.lower()

    if test:

      filename = f"{file_low}_test"

    else:

      filename = file_low

    upload_file = S3FileTransferOperator(
      task_id=f"upload_{filename}",
      source_base_path=conf['prod_data_dir'],
      source_key=f"{filename}.csv",
      dest_s3_bucket=conf['dest_s3_bucket'],
      dest_s3_conn_id=conf['default_s3_conn_id'],
      dest_s3_key=f"city_docs/{filename}.csv",
      replace=True,
      on_failure_callback=notify,
      on_retry_callback=notify,
      on_success_callback=notify,
      dag=dag_subdag,
    )

  return dag_subdag

def upload_div_files_subdag(files,parent_dag,test):
  """
  Generate a DAG to be used as a subdag 
  that uploads city docs files 
  """

  conf_name = schedule.get(parent_dag)

  dag_subdag = DAG(
    dag_id=f'{parent_dag}.upload_div_files',
    default_args=args,
    start_date=parent_start[parent_dag],
    schedule_interval=parent_sched[parent_dag],
    catchup=False
  )

  for file in files:

    filename = file.lower()

    upload_file = S3FileTransferOperator(
      task_id=f"upload_{filename}",
      source_base_path=conf['prod_data_dir'],
      source_key=f"{filename}.csv",
      dest_s3_bucket=conf['dest_s3_bucket'],
      dest_s3_conn_id=conf['default_s3_conn_id'],
      dest_s3_key=f"city_docs/{filename}.csv",
      replace=True,
      on_failure_callback=notify,
      on_retry_callback=notify,
      on_success_callback=notify,
      dag=dag_subdag,
    )

  return dag_subdag
