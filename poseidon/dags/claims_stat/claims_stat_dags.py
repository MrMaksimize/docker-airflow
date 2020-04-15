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
email_recips = conf['mail_notify_claims']


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
    on_failure_callback=afsys_send_email,
    dag=dag)


#: Upload clean and geocode claims data
clean_geocode = PythonOperator(
    task_id='clean_geocode_claims',
    python_callable=clean_geocode_claims,
    on_failure_callback=afsys_send_email,
    dag=dag)


#: Upload prod claims file to S3
upload_claimstat_clean = S3FileTransferOperator(
    task_id='upload_claimstat_clean',
    source_base_path=conf['prod_data_dir'],
    source_key='claim_stat_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='risk/claims_clean_datasd_v1.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)

upload_addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_claims_address_book',
    source_base_path=conf['temp_data_dir'],
    source_key='claims_address_book.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['ref_s3_bucket'],
    dest_s3_key='claims_address_book.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)


#: Deploy Dashboard
deploy_dashboard = BashOperator(
    task_id='deploy_dashboard',
    bash_command=deploy_dashboard(),
    on_failure_callback=afsys_send_email,
    dag=dag)


#: send file update email to interested parties
send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    task_id='send_dashboard_updated',
    to=email_recips,
    subject='Dashboard Updated',
    file_url=f"https://sandiego-panda.shinyapps.io/claims_{conf['env'].lower()}/",
    message='<p>The ClaimStat tool has been updated.</p>' \
            + '<p>Please follow the link below to view the tool.</p>',
    on_failure_callback=afsys_send_email,
    dag=dag)

get_claims_data >> clean_geocode >> [upload_claimstat_clean,upload_addresses_to_S3]
upload_claimstat_clean >> deploy_dashboard >> send_last_file_updated_email
