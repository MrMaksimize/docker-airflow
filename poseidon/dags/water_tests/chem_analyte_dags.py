"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.water_tests.chem_analyte_jobs import *
from dags.water_tests.chem_analyte_subdags import *

# Optional operator imports
from airflow.operators.subdag_operator import SubDagOperator

# Required variables

args = general.args
conf = general.config
schedule = general.schedule['chem_analytes']
start_date = general.start_date['chem_analytes']

#: Required DAG definition
dag = DAG(dag_id='chem_analytes',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Query Oracle for records and process prod file
process_drinking_water = SubDagOperator(
    task_id='get_create_drinking',
    subdag=get_create_drinking_subdag(),
    dag=dag,
  )

#: Query Oracle for records and process prod file
process_plant_effluent = SubDagOperator(
    task_id='get_create_plants',
    subdag=get_create_plants_subdag(),
    dag=dag,
  )
