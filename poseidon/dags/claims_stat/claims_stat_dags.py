"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
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
start_date = general.start_date['ttcs']


#: Dag definition
dag = DAG(dag_id='claims_stat', default_args=args, start_date=start_date, schedule_interval=schedule['ttcs'])


deploy_dashboard = RShinyDeployOperator(
    task_id='deploy_dashboard',
    shiny_appname="claims_{}".format(conf['env']),
    shiny_path="{}/claims_stat/claims.Rmd".format(conf['dags_dir']),
    shiny_acct_name=conf['shiny_acct_name'],
    shiny_token=conf['shiny_token'],
    shiny_secret=conf['shiny_secret'],
    force= "TRUE",
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)
