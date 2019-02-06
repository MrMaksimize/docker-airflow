"""Special Events _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from poseidon.util import general
from poseidon.util.notifications import notify
from poseidon.dags.special_events.se_jobs import *
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['special_events']

#: Dag spec
dag = DAG(dag_id='special_events',
          default_args=args,
          schedule_interval=schedule)


#: Latest Only Operator for special events
se_latest_only = LatestOnlyOperator(task_id='se_latest_only', dag=dag)


#: Get special events from DB
get_special_events = PythonOperator(
    task_id='get_special_events',
    python_callable=get_special_events,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process and geocode raw special events file
process_special_events = PythonOperator(
    task_id='process_special_events',
    python_callable=process_special_events,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod SE file to S3
upload_special_events = S3FileTransferOperator(
    task_id='upload_special_events',
    source_base_path=conf['prod_data_dir'],
    source_key='special_events_list_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='special_events/special_events_list_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_special_events_md = get_seaboard_update_dag('special-events.md', dag)

#: Execution rules

#: se_latest_only must run before get_special_events
get_special_events.set_upstream(se_latest_only)

#: process_special_events dependent on get_special_events
process_special_events.set_upstream(get_special_events)

#: upload_special_events dependent on process_special_events
upload_special_events.set_upstream(process_special_events)

#: update github modified date after S3 upload
update_special_events_md.set_upstream(upload_special_events)
