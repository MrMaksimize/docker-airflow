from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general

from dags.pv_production.pv_prod_jobs import *

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = general.schedule['pv_prod']
start_date = general.start_date['pv_prod']
conf = general.config

dag = DAG(
    dag_id='pv_prod',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Downloads latest 1:40min of PV data from API
get_pv_data_write_temp = PythonOperator(
    task_id='get_pv_data_write_temp',
    python_callable=get_pv_data_write_temp,
    provide_context=True,
    dag=dag)

#: Joins downloaded files from API to production
update_pv_prod = PythonOperator(
    task_id='update_pv_prod',
    python_callable=update_pv_prod,
    provide_context=True,
    dag=dag)

#: TODO
get_lucid_token = PythonOperator(
    task_id='get_lucid_token',
    python_callable=get_lucid_token,
    provide_context=True,
    dag=dag)

#: TODO
push_lucid_data = PythonOperator(
    task_id='push_lucid_data',
    python_callable=push_lucid_data,
    provide_context=True,
    dag=dag)

#: Operator to control only one S3 upload a day
check_upload_time = ShortCircuitOperator(
    task_id='check_upload_time',
    provide_context=True,
    python_callable=check_upload_time,
    dag=dag)

#: Uploads the pv production file
s3_upload = S3FileTransferOperator( # creating a different upload object for each...
    task_id='s3_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='pv_production.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key=f'pv_production/pv_production.csv',
    replace=True,
    dag=dag)

#: Update portal modified date
update_pv_md = get_seaboard_update_dag('pv_production.md', dag)

#: Execution rules
get_pv_data_write_temp >> [update_pv_prod,get_lucid_token]
update_pv_prod >> check_upload_time >> s3_upload >> update_pv_md
get_lucid_token >> push_lucid_data