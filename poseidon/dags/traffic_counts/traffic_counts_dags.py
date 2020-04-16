"""Traffic counts _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from dags.traffic_counts.traffic_counts_jobs import *
from trident.util import general
from trident.util.notifications import afsys_send_email

from trident.util.seaboard_updates import *


args = general.args
conf = general.config
schedule = general.schedule['traffic_counts']
start_date = general.start_date['traffic_counts']


dag = DAG(
    dag_id='traffic_counts',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)


#: Downloads traffic counts xlsx from share
get_traffic_counts = PythonOperator(
    task_id='get_traffic_counts',
    python_callable=get_traffic_counts,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag)

#: Cleans the downloaded XLSX file, converts it to CSV data.
clean_traffic_counts = PythonOperator(
    task_id='clean_traffic_counts',
    python_callable=clean_traffic_counts,
    on_failure_callback=afsys_send_email,
    dag=dag
)

#: Builds the prod file
build_traffic_counts = PythonOperator(
    task_id='build_traffic_counts',
    python_callable=build_traffic_counts,
    on_failure_callback=afsys_send_email,
    dag=dag
)

#: Uploads the generated production file
upload_traffic_counts = S3FileTransferOperator(
    task_id='upload_traffic_counts',
    source_base_path=conf['prod_data_dir'],
    source_key='traffic_counts_datasd_v1.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='traffic_counts/traffic_counts_datasd_v1.csv',
    replace=True,
    on_failure_callback=afsys_send_email,
    dag=dag)

update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'traffic_volumes'},
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Update portal modified date
update_traffic_md = get_seaboard_update_dag('traffic-volumes.md', dag)

#: Execution Rules
get_traffic_counts >> clean_traffic_counts >> build_traffic_counts >> upload_traffic_counts >> [update_json_date,update_traffic_md]
