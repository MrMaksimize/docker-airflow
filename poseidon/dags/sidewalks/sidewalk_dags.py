"""Sidewalk _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email


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
    
    dag=dag)

#: Upload OCI file to S3
upload_oci_file = S3FileTransferOperator(
    task_id='upload_oci',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalk_cond_datasd_v1.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='tsw/sidewalk_cond_datasd_v1.csv',
    
    replace=True,
    dag=dag)


#: Execution order

get_sidewalk_data >> upload_oci_file