"""PD collisions subdags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general
from dags.pd.pd_col_jobs import *

from trident.util.seaboard_updates import *
conf = general.config
args = general.args
schedule = general.schedule['pd_col']
start_date = general.start_date['pd_col']

def get_files_subdag():
  """
  Generate a DAG to be used as a subdag 
  that downloads collisions files 
  """

  dag_subdag = DAG(
    dag_id='pd_col.get_files',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )  

  get_activity_data = PythonOperator(
    task_id="get_activities",
    provide_context=True,
    python_callable=get_collisions_data,
    op_kwargs={'mode': 'activities'},
    dag=dag_subdag,
  )

  get_details_data = PythonOperator(
    task_id="get_details",
    provide_context=True,
    python_callable=get_collisions_data,
    op_kwargs={'mode': 'details'},
    dag=dag_subdag,
  )

  return dag_subdag