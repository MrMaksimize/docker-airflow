""" Google Analytics _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.google_analytics.google_analytics_all_jobs import *
from dags.google_analytics.ga_all_subdags import *

# Optional operator imports
from airflow.operators.subdag_operator import SubDagOperator

# Required variables

args = general.args
conf = general.config
schedule = general.schedule['ga_portal']
start_date = general.start_date['ga_portal']

#: Required DAG definition
dag = DAG(dag_id='ga_sandiego',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Authenticate GA
create_keyfile = PythonOperator(
    task_id='create_client_secrets',
    python_callable=create_client_secrets,
    dag=dag)

#: Get various reports in subdag
reports_subdag = SubDagOperator(
    task_id='get_upload_reports',
    subdag=create_subdag_operators(),
    dag=dag,
  )

create_keyfile >> reports_subdag