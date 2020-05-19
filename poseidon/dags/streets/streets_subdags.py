"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email


from dags.streets.streets_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['streets']
start_date = general.start_date['streets']

files = ['completed','in_progress','planned']

def esri_layer_subdag():
  """
  Generate a DAG to be used as a subdag 
  that updates ESRI map layers
  """

  dag_subdag = DAG(
    dag_id='streets.write_esri_layers',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  for file in files:

    write_esri_layer = PythonOperator(
        task_id=f"send_arcgis_{file}",
        provide_context=True,
        python_callable=send_arcgis,
        op_kwargs={'mode': file},
        
        dag=dag_subdag,
      )

  return dag_subdag