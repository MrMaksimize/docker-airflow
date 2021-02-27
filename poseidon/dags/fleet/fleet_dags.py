"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.bash_operator import BashOperator
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
delays = PythonOperator(
    task_id='query_fleet_delays',
    python_callable=get_delays,
    dag=dag)

#: Query Fleet Focus jobs table
jobs = PythonOperator(
    task_id='query_fleet_jobs',
    python_callable=get_jobs,
    dag=dag)

#: Query Fleet Focus eq main table
vehicles = PythonOperator(
    task_id='query_fleet_vehicles',
    python_callable=get_vehicles,
    dag=dag)

vehicles_process = BashOperator(
    task_id='create_valid_veh',
    bash_command='Rscript /usr/local/airflow/poseidon/dags/fleet/valid_veh.R',
    dag=dag)

#: Query Fleet Focus eq main table
availability = PythonOperator(
    task_id='query_availability',
    python_callable=get_availability,
    dag=dag)

#: Query Fleet Focus eq main table
avail_calc = PythonOperator(
    task_id='avail_calc',
    python_callable=calc_availability,
    provide_context=True,
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

upload_valid_vehicles = S3FileTransferOperator(
    task_id=f'upload_valid_veh',
    source_base_path=conf['prod_data_dir'],
    source_key=f'fleet_valid_vehicles.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'fleet/fleet_valid_vehicles.csv',
    replace=True,
    dag=dag)

upload_avail_vehicles = S3FileTransferOperator(
    task_id=f'upload_avail_veh',
    source_base_path=conf['prod_data_dir'],
    source_key=f'fleet_avail_vehs.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'fleet/fleet_avail_vehs.csv',
    replace=True,
    dag=dag)

#: Required execution rules
delays >> upload_delays
jobs >> upload_jobs
vehicles >> upload_vehicles
vehicles >> vehicles_process >> upload_valid_vehicles
availability >> avail_calc >> upload_avail_vehicles
