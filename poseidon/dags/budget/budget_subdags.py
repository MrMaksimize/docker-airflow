"""Budget subdags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.budget.budget_jobs import *

import os
conf = general.config
args = general.args
schedule = general.schedule['budget']
start_date = general.start_date['budget']

modes = ['Budget','Actuals']

paths = [
'Capital/P-T-D',
'Capital/FY',
'Operating'
]

prod_dir = conf['prod_data_dir']

ref_files = ['projects','funds','depts','accounts']

def get_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that downloads budget files 
  """

  dag_subdag = DAG(
    dag_id='budget.get_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for mode in modes:

    for path in paths:

      mode_str = mode.lower()
      path_str = path.lower().replace('/','_').replace('-','')

      task = f"{mode_str}_" \
      + f"{path_str}"

      create_file = PythonOperator(
        task_id=f"create_{task}",
        python_callable=get_budget_files,
        op_kwargs={'mode': mode_str, 'path':path_str},
        
        dag=dag_subdag,
      )

  return dag_subdag

def create_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that downloads budget files 
  """

  dag_subdag = DAG(
    dag_id='budget.create_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for mode in modes:

    for path in paths:

      mode_str = mode.lower()
      path_str = path.lower().replace('/','_').replace('-','')

      task = f"{mode_str}_" \
      + f"{path_str}"

      PythonOperator(
        task_id=f'create_{mode_str}_{path_str}',
        python_callable=create_file,
        op_kwargs={'mode': mode_str, 'path':path_str},
        
        dag=dag_subdag)

  return dag_subdag


def upload_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that uploads budget datasets 
  """

  dag_subdag = DAG(
    dag_id='budget.upload_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for mode in modes:

    for path in paths:

      mode_str = mode.lower()
      path_str = path.lower().replace('/','_').replace('-','')

      task = f"{mode_str}_" \
      + f"{path_str}"

      #: Upload budget files to S3
      upload_task = S3FileTransferOperator(
          task_id=f'upload_{task}',
          source_base_path=conf['prod_data_dir'],
          source_key=f'{task}_datasd.csv',
          dest_s3_conn_id=conf['default_s3_conn_id'],
          dest_s3_bucket=conf['dest_s3_bucket'],
          dest_s3_key=f'budget/{task}_datasd.csv',
          
          replace=True,
          dag=dag_subdag)

  return dag_subdag

def upload_ref_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that downloads budget files 
  """

  dag_subdag = DAG(
    dag_id='budget.upload_refs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for ref in ref_files:
    #: Upload budget files to S3
    upload_task = S3FileTransferOperator(
      task_id=f'upload_{ref}_ref',
      source_base_path=conf['prod_data_dir'],
      source_key=f'budget_reference_{ref}_datasd_v1.csv',
      dest_s3_conn_id=conf['default_s3_conn_id'],
      dest_s3_bucket=conf['dest_s3_bucket'],
      dest_s3_key=f'budget/budget_reference_{ref}_datasd_v1.csv',
      
      replace=True,
      dag=dag_subdag)

  return dag_subdag