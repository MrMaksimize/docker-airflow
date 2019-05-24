from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from dags.cityiq.cityiqjobs import get_token_response, get_assets, get_asset_details, get_pkout_bbox, get_pkin_bbox
from trident.util import general
from trident.util.notifications import notify
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from datetime import *


# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
start_date = general.start_date['cityiq'] 
#: Dag spec
dag = DAG(dag_id='cityiq', default_args=args, start_date=start_date, schedule_interval=general.schedule['cityiq'])

get_token_response = PythonOperator(
    task_id='get_token_response',
    python_callable=get_token_response,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_assets = PythonOperator(
    task_id='get_assets',
    python_callable=get_assets,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_asset_details = PythonOperator(
    task_id='get_asset_details',
    python_callable=get_asset_details,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_pkout_bbox = PythonOperator(
    task_id='get_pkout_bbox',
    python_callable=get_pkout_bbox,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)
    
get_pkin_bbox = PythonOperator(
    task_id='get_pkin_bbox',
    python_callable=get_pkin_bbox,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_pkin_bbox = PythonOperator(
    task_id='get_pedvt_bbox',
    python_callable=get_pkin_bbox,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)
#'/usr/local/airflow/poseidon/dags/cityiq/assets.json'
#conf['temp_data_dir']

#set upstream 

prefixes = ['pkin', 'pkout']
for prefix in prefixes:
    file_time = datetime.now().strftime('%Y_%m_%d_')
    file_name = file_time + prefix + '.json'
    upload = S3FileTransferOperator(
            task_id='upload_bbox',
            source_base_path='/data/temp/' + prefix,
            source_key=file_name,
            dest_s3_conn_id=conf['default_s3_conn_id'],
            dest_s3_bucket=conf['dest_s3_bucket'],
            dest_s3_key='cityiq/' + prefix + '/' + file_name,
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            replace=True,
            dag=dag)
    # set upstream or try storing in a list