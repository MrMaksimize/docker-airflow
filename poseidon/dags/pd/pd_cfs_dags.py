"""Police calls_for_service _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from dags.pd.pd_cfs_jobs import *
from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date
import os

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['pd_cfs']

dag = DAG(
    dag_id='pd_cfs', default_args=args, start_date=start_date, schedule_interval=schedule['pd_cfs'])


#: Get CFS data from FTP and save to temp folder
get_cfs_data = PythonOperator(
    task_id='get_cfs_data',
    python_callable=get_cfs_data,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process CFS data and save result to prod folder
process_cfs_data = PythonOperator(
    task_id='process_cfs_data',
    python_callable=process_cfs_data,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod file to S3
cfs_to_S3 = S3FileTransferOperator(
    task_id='cfs_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='pd_calls_for_service_2020_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='pd/pd_calls_for_service_2020_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'police_calls_for_service'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_pd_cfs_md = get_seaboard_update_dag('police-calls-for-service.md', dag)

#: Execution rules:

get_cfs_data >> process_cfs_data >> cfs_to_S3 >> [update_pd_cfs_md,update_json_date]
