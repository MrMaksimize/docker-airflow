"""Police calls_for_service _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from dags.pd.pd_cfs_jobs import *
from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag


args = general.args
conf = general.config
schedule = general.schedule

dag = DAG(
    dag_id='pd_cfs', default_args=args, schedule_interval=schedule['pd_cfs'])


#: Latest Only Operator for pd_cfs
pd_cfs_latest_only = LatestOnlyOperator(
    task_id='pd_cfs_latest_only', dag=dag)


#: Get CFS data from FTP and save to temp folder
get_cfs_data = BashOperator(
    task_id='get_cfs_data',
    bash_command=get_cfs_data(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process CFS data and save result to prod folder
process_cfs_data = PythonOperator(
    task_id='process_cfs_data',
    python_callable=process_cfs_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod file to S3
cfs_to_S3 = S3FileTransferOperator(
    task_id='cfs_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='pd_calls_for_service_'+curr_year+'_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='pd/pd_calls_for_service_'+curr_year+'_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_pd_cfs_md = get_seaboard_update_dag('police-calls-for-service.md', dag)

#: Execution rules:

#: pd_cfs_latest_only must run before pd_cfs_data
get_cfs_data.set_upstream(pd_cfs_latest_only)

#: Data processing is triggered after data retrieval.
process_cfs_data.set_upstream(get_cfs_data)

#: Data upload to S3 is triggered after data processing completion.
cfs_to_S3.set_upstream(process_cfs_data)

#: Github update depends on S3 upload success.
update_pd_cfs_md.set_upstream(cfs_to_S3)
