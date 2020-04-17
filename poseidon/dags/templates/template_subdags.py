"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email


from dags.templates.template_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['streets']
start_date = general.start_date['streets']

tasks = ['file1','file2','file3']

def create_subdag_operators():
  """
  Generate a DAG to be used as a subdag 
  that updates ESRI map layers
  """

  dag_subdag = DAG(
    dag_id='template.template_subdag',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for index, task in enumerate(tasks):

    set_of_tasks = PythonOperator(
        task_id=f"subdag_tasks_{index}",
        provide_context=True,
        python_callable=task_for_subdag,
        op_kwargs={'mode': index},
        
        dag=dag_subdag,
      )

  return dag_subdag