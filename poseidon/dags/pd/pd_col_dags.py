"""PD collisions _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from dags.pd.pd_col_jobs import *
from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

args = general.args
conf = general.config
schedule = general.schedule

dag = DAG(
    dag_id='pd_col', default_args=args, schedule_interval=schedule['pd_col'])


#: Latest Only Operator for pd_col
pd_col_latest_only = LatestOnlyOperator(
    task_id='pd_col_latest_only', dag=dag)

##: Get collisions data from FTP and save to temp folder
get_collisions_data = PythonOperator(
    task_id='get_collisions_data',
    python_callable=get_collisions_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process collisions data and save result to prod folder
process_collisions_data = PythonOperator(
    task_id='process_collisions_data',
    python_callable=process_collisions_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod file to S3
collisions_to_S3 = S3FileTransferOperator(
    task_id='collisions_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='pd_collisions_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='pd/pd_collisions_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_pd_cls_md = get_seaboard_update_dag('police-collisions.md', dag)

#: Execution rules:

#: pd_col_latest_only must run before get_collisions_data
get_collisions_data.set_upstream(pd_col_latest_only)

#: Data processing is triggered after data retrieval.
process_collisions_data.set_upstream(get_collisions_data)

#: Data upload to S3 is triggered after data processing completion.
collisions_to_S3.set_upstream(process_collisions_data)

#: Github update depends on S3 upload success.
update_pd_cls_md.set_upstream(collisions_to_S3)
