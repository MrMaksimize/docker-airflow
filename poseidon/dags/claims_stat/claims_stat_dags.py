"""This module contains dags and tasks for extracting data out of Claims Stat."""
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG

from dags.claims_stat.claims_stat_jobs import *
from dags.claims_stat.claims_stat_subdags import *

from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email
from trident.operators.r_operator import RScriptOperator
from trident.operators.r_operator import RShinyDeployOperator
from trident.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['claims_stat']

#: Dag definition
dag = DAG(dag_id='claims_stat',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['claims_stat'],
    catchup=False
    )

get_claims = PythonOperator(
    task_id='get_claims_data',
    python_callable=get_claims_data,
    dag=dag)

clean_geocode = PythonOperator(
    task_id='clean_geocode_claims',
    python_callable=clean_geocode_claims,
    dag=dag)

upload_addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_claims_address_book',
    source_base_path=conf['prod_data_dir'],
    source_key='claims_address_book.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/claims_address_book.csv',
    replace=True,
    dag=dag)

upload_deploy_email = SubDagOperator(
    task_id='upload_deploy_email',
    subdag=create_subdag(),
    dag=dag)

get_claims >> clean_geocode >> [upload_addresses_to_S3,upload_deploy_email]

