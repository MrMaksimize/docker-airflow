from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from dags.cityiq.cityiqjobs import *
from trident.util import general
from trident.util.notifications import afsys_send_email

from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime


# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
start_date = general.start_date['cityiq'] 
#: Dag spec
dag = DAG(
    dag_id='cityiq',
    default_args=args, 
    start_date=start_date, 
    schedule_interval=general.schedule['cityiq'],
    catchup=False
    )

get_token_response = PythonOperator(
    task_id = 'get_token_response',
    python_callable=get_token_response,
    
    dag=dag)

get_parking_bbox = PythonOperator(
    task_id='get_parking_bbox',
    provide_context=True,
    python_callable=get_events,
    
    dag=dag)

event_files = ["pkin","pkout"]

for file in event_files:
    file_time = datetime.now().strftime('%Y_%m_%d_') 
    file_name = f'{file_time}{file}.json'
    s3_upload = S3FileTransferOperator( # creating a different upload object for each...
        task_id=f'upload_{file}',
        source_base_path=conf['prod_data_dir'],
        source_key=file_name,
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key=f'cityiq/{file_name}',
        
        replace=True,
        dag=dag)

    #: Upload after getting events
    get_parking_bbox >> s3_upload
    

#: Execution Rules
get_token_response >> get_parking_bbox