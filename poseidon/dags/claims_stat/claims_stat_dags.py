"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

from dags.claims_stat.claims_stats_jobs import *

from datetime import datetime, timedelta
from trident.util import general
from trident.operators.r_operator import RScriptOperator
from trident.operators.r_operator import RShinyDeployOperator
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag
import os
import glob

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['claims_stat']


#: Dag definition
dag = DAG(dag_id='claims_stat', default_args=args, start_date=start_date, schedule_interval=schedule['claims_stat'])


#: Latest Only Operator for claims
claims_stat_latest_only = LatestOnlyOperator(
    task_id='claims_stat_latest_only', dag=dag)

#: Pull claims data from oracle
get_claims_data = PythonOperator(
    task_id='get_claims_data',
    python_callable=get_claims_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Upload clean and geocode claims data
clean_geocode = PythonOperator(
    task_id='clean_geocode_claims',
    python_callable=clean_geocode_claims,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Upload prod claims file to S3
upload_claimstat_clean = S3FileTransferOperator(
    task_id='upload_claimstat_clean',
    source_base_path=conf['prod_data_dir'],
    source_key='claimstat_clean.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='risk/claims_clean_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)



#: Deploy Dashboard
deploy_dashboard = RShinyDeployOperator(
    task_id='claims_stat_deploy_dashboard',
    shiny_appname="claims_{}".format(conf['env'].lower()),
    shiny_path="{}/claims_stat/claims.Rmd".format(conf['dags_dir']),
    shiny_acct_name=conf['shiny_acct_name'],
    shiny_token=conf['shiny_token'],
    shiny_secret=conf['shiny_secret'],
    force= "TRUE",
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Get Claims Data runs after latest_only check
get_claims_data.set_upstream(claims_stat_latest_only)
#: Clean geocode depends on raw oracle data coming in
clean_geocode.set_upstream(get_claims_data)
#: Upload claims data after geocoding
upload_claimstat_clean.set_upstream(clean_geocode)
#: Upload must complete before running shiny build
deploy_dashboard.set_upstream(upload_claimstat_clean)
