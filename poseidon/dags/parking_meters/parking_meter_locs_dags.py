"""Parking meters _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.parking_meters.parking_meter_locs_jobs import *
from trident.util.seaboard_updates import *

args = general.args
schedule = general.schedule['parking_meter_locs']
start_date = general.start_date['parking_meter_locs']
conf = general.config

dag = DAG(
    dag_id='parking_meter_locs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Downloads all parking files from FTP
get_parking_files = PythonOperator(
    task_id='get_meter_loc_files',
    provide_context=True,
    python_callable=ftp_download,
    
    dag=dag)

#: Joins downloaded files from ftp to production
build_prod_file = PythonOperator(
    task_id='build_prod_file',
    provide_context=True,
    python_callable=build_prod_file,
    
    dag=dag)

#clean_daily_files = PythonOperator(
    #task_id='clean_files',
    #provide_context=True,
    #python_callable=clean_files,
    #dag=dag
    #)

#: Uploads the generated production file
upload_prod_file = S3FileTransferOperator(
    task_id='upload_meter_locs',
    source_base_path=conf['prod_data_dir'],
    source_key='treas_parking_meters_loc_datasd_v1.csv',
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key='parking_meters/treas_parking_meters_loc_datasd_v1.csv',
    replace=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'parking_meters_locations'},
    dag=dag)

#: Update portal modified date
update_parking_trans_md = get_seaboard_update_dag('parking-meters-locations.md', dag)

#: Execution Rules
get_parking_files >> build_prod_file >> upload_prod_file >> [update_parking_trans_md, update_json_date]
