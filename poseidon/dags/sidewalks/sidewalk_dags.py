"""Sidewalk _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from dags.sidewalks.sidewalk_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['sidewalks']
start_date = general.start_date['sidewalks']

#: Dag spec
dag = DAG(dag_id='sidewalk', 
    default_args=args, 
    start_date=start_date, 
    schedule_interval=schedule,
    catchup=False
    )

#: Get sidewalk data from DB
get_sidewalk_data = PythonOperator(
    task_id='get_sidewalk_oci',
    python_callable=get_sidewalk_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload OCI file to S3
upload_oci_file = S3FileTransferOperator(
    task_id='upload_oci',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalk_cond_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalk_cond_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)


#: Execution order

get_sidewalk_data >> upload_oci_file