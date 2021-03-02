"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general

# Optional import if uploading within subdag
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator


from dags.water_tests.chem_analyte_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['chem_analytes']
start_date = general.start_date['chem_analytes']

def get_create_drinking_subdag():
  """
  Generate a DAG to be used as a subdag 
  that queries Oracle for drinking water
  and plant effluent chem tests
  """
  dag_subdag = DAG(
    dag_id=f'chem_analytes.get_create_drinking',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  query_oracle = PythonOperator(
      task_id=f"get_drinking_sql",
      provide_context=True,
      python_callable=get_oracle_data,
      op_kwargs={'mode': 'drinking'},
      dag=dag_subdag,
    )

  create_prod = PythonOperator(
      task_id=f"create_drinking_prod",
      provide_context=True,
      python_callable=process_data,
      op_kwargs={'mode': 'drinking'},
      dag=dag_subdag,
    )

  upload_prod = S3FileTransferOperator(
    task_id=f'upload_drinking',
    source_base_path=conf['prod_data_dir'],
    source_key=f'analyte_tests_drinking_water_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'pud/chem/analyte_tests_drinking_water_datasd.csv',
    replace=True,
    dag=dag_subdag)

  query_oracle >> create_prod >> upload_prod

  return dag_subdag

def get_create_plants_subdag():
  """
  Generate a DAG to be used as a subdag 
  that queries Oracle for drinking water
  and plant effluent chem tests
  """
  dag_subdag = DAG(
    dag_id=f'chem_analytes.get_create_plants',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  query_oracle = PythonOperator(
      task_id=f"get_plants_sql",
      provide_context=True,
      python_callable=get_oracle_data,
      op_kwargs={'mode': 'plants'},
      dag=dag_subdag,
    )

  create_prod = PythonOperator(
      task_id=f"create_plants_prod",
      provide_context=True,
      python_callable=process_data,
      op_kwargs={'mode': 'plants'},
      dag=dag_subdag,
    )

  upload_prod = S3FileTransferOperator(
    task_id=f'upload_plants',
    source_base_path=conf['prod_data_dir'],
    source_key=f'analyte_tests_effluent_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'pud/chem/analyte_tests_effluent_datasd.csv',
    replace=True,
    dag=dag_subdag)

  query_oracle >> create_prod >> upload_prod

  return dag_subdag