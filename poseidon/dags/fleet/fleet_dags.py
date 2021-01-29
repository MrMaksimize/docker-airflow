"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.fleet.fleet_jobs import *
#from dags.fleet.fleet_subdags import *

# Required variables

args = general.args
conf = general.config
schedule = general.schedule['fleet']
start_date = general.start_date['fleet']

#: Required DAG definition
dag = DAG(dag_id='fleet_focus',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Query Fleet Focus delays table
get_delays = PythonOperator(
    task_id='query_fleet_delays',
    python_callable=get_delays,
    dag=dag)

#: Query Fleet Focus jobs table
get_jobs = PythonOperator(
    task_id='query_fleet_jobs',
    python_callable=get_jobs,
    dag=dag)

#: Query Fleet Focus eq main table
get_vehicles = PythonOperator(
    task_id='query_fleet_vehicles',
    python_callable=get_vehicles,
    dag=dag)

upload_delays = S3FileTransferOperator(
    task_id=f'upload_fleet_delays',
    source_base_path=conf['prod_data_dir'],
    source_key=f'fleet_delays.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'fleet/fleet_delays.csv',
    replace=True,
    dag=dag)

upload_jobs = S3FileTransferOperator(
    task_id=f'upload_fleet_jobs',
    source_base_path=conf['prod_data_dir'],
    source_key=f'fleet_jobs.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'fleet/fleet_jobs.csv',
    replace=True,
    dag=dag)

upload_vehicles = S3FileTransferOperator(
    task_id=f'upload_fleet_veh',
    source_base_path=conf['prod_data_dir'],
    source_key=f'fleet_vehicles.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'fleet/fleet_vehicles.csv',
    replace=True,
    dag=dag)

#: Required execution rules
get_delays >> upload_delays
get_jobs >> upload_jobs
get_vehicles >> upload_vehicles
