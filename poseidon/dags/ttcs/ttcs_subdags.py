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

queries = ['main','location','dba','phone']

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

  for query in queries:

    #: Get active businesses and save as .csv to temp folder
    execute_query = PythonOperator(
        task_id=f'query_{query}',
        python_callable=query_ttcs,
        provide_context=True,
        op_kwargs={'mode': query},
        dag=dag_subdag)

  return dag_subdag