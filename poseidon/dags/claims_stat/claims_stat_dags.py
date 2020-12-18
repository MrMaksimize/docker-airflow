"""This module contains dags and tasks for extracting data out of Claims Stat."""
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

from dags.claims_stat.claims_stats_jobs import *

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

#: Pull claims data from oracle
get_claims_data = PythonOperator(
    task_id='get_claims_data',
    python_callable=get_claims_data,
    
    dag=dag)


#: Upload clean and geocode claims data
clean_geocode = PythonOperator(
    task_id='clean_geocode_claims',
    python_callable=clean_geocode_claims,
    
    dag=dag)


#: Upload prod claims file to S3
upload_claimstat_clean = S3FileTransferOperator(
    task_id='upload_claimstat_clean',
    source_base_path=conf['prod_data_dir'],
    source_key='claim_stat_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='risk/claims_clean_datasd_v1.csv',
    
    replace=True,
    dag=dag)

upload_addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_claims_address_book',
    source_base_path=conf['temp_data_dir'],
    source_key='claims_address_book.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='claims_address_book.csv',
    
    replace=True,
    dag=dag)


#: Deploy Dashboard
deploy_dashboard = BashOperator(
    task_id='deploy_dashboard',
    bash_command=deploy_dashboard(),
    
    dag=dag)


#: send file update email to interested parties
send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    task_id='send_dashboard_updated',
    to="{{ var.value.MAIL_NOTIFY_CLAIMS }}",
    subject='Dashboard Updated',
    file_url=f"https://sandiego-panda.shinyapps.io/claims_{conf['env'].lower()}/",
    message='<p>The ClaimStat tool has been updated.</p>' \
            + '<p>Please follow the link below to view the tool.</p>',
    
    dag=dag)

get_claims_data >> clean_geocode >> [upload_claimstat_clean,upload_addresses_to_S3]
upload_claimstat_clean >> deploy_dashboard >> send_last_file_updated_email
