from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from dags.cityiq.cityiqjobs import *
from trident.util import general
from trident.util.notifications import notify
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime
import glob
import os


# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
start_date = general.start_date['pk_events'] 
#: Dag spec
dag = DAG(
    dag_id='pk_events',
    default_args=args, 
    start_date=start_date, 
    schedule_interval=general.schedule['pk_events'])

get_token_response = PythonOperator(
    task_id = 'get_token_response',
    python_callable=get_token_response,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_parking_bbox = PythonOperator(
    task_id='get_parking_bbox',
    provide_context=True,
    python_callable=get_events,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

event_files = ['pkin','pkout']
#file_date = {{ execution_date }}


for file in event_files:
    file_pattern = f"{conf['prod_data_dir']}/{file}_*.json"
    list_of_files = glob.glob(file_pattern)
    if len(list_of_files) > 0:
        latest_file = max(list_of_files, key=os.path.getmtime)
    else:
        latest_file = None
    file_name = os.path.basename(latest_file)
    s3_upload = S3FileTransferOperator(
        task_id=f'upload_{file}',
        source_base_path=conf['prod_data_dir'],
        source_key=file_name,
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key=f'cityiq/{file_name}',
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)

    #: Upload after getting events
    s3_upload << get_parking_bbox
    

#: Execution Rules

get_token_response >> get_parking_bbox


    