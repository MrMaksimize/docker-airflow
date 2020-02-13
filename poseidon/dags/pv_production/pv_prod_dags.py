from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import notify

from dags.pv_production.pv_prod_jobs import *

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = general.schedule['pv_prod']
start_date = general.start_date['pv_prod']
conf = general.config

currTime = context['execution_date'].in_timezone('America/Los_Angeles')

dag = DAG(
    dag_id='pv_prod',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule
    )

#: Downloads latest 1:40min of PV data from API
get_pv_data_write_temp = PythonOperator(
    task_id='get_pv_data_write_temp',
    python_callable=get_pv_data_write_temp,
    op_kwargs={'currTime': currTime},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Joins downloaded files from API to production
update_pv_prod = PythonOperator(
    task_id='update_pv_prod',
    python_callable=update_pv_prod,
    op_kwargs={'currTime': currTime},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Uploads the pv production file
s3_upload = S3FileTransferOperator( # creating a different upload object for each...
    task_id='s3_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='pv_production.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key=f'pv_production/pv_production.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

get_pv_data_write_temp >> update_pv_prod >> s3_upload