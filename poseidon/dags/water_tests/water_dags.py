"""Water _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from poseidon.util import general
from poseidon.util.notifications import notify

from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator

from poseidon.dags.water_tests.indicator_bacteria_jobs import get_indicator_bacteria_tests
from poseidon.dags.water_tests.indicator_bacteria_jobs import get_latest_bac_tests
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

args = general.args
conf = general.config


dag = DAG(
    dag_id='indicator_bacteria_tests',
    default_args=args,
    schedule_interval=general.schedule['indicator_bacteria_tests'])


#: Latest Only Operator for traffic_counts
wtr_latest_only = LatestOnlyOperator(task_id='water_latest_only', dag=dag)


# TODO - teach me how to be yearly
# Pull out all indicator bac tests.
get_indicator_bac_tests = PythonOperator(
    task_id='get_indicator_bac_tests',
    python_callable=get_indicator_bacteria_tests,
    op_kwargs={
        'date_start': '01-JUN-2014',
        'date_end': (datetime.now() + timedelta(days=5)).strftime('%d-%b-%Y')
    },
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

# Get last bacteria tests for any given point.
get_latest_bac_tests = PythonOperator(
    task_id='get_latest_bac_tests',
    python_callable=get_latest_bac_tests,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

# Uploads the indicator bacteria tests full result.
upload_indicator_bac_tests = S3FileTransferOperator(
    task_id='upload_indicator_bac_tests',
    source_base_path=conf['prod_data_dir'],
    source_key='indicator_bacteria_tests_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='water_testing/indicator_bacteria_tests_datasd.csv',
    replace=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

# Uploads the latest indicator bacteria tests.
upload_latest_indicator_bac_tests = S3FileTransferOperator(
    task_id='upload_latest_indicator_bac_tests',
    source_base_path=conf['prod_data_dir'],
    source_key='latest_indicator_bac_tests_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='water_testing/latest_indicator_bac_tests_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_water_md = get_seaboard_update_dag(
    'monitoring-of-indicator-bacteria-in-drinking-water.md',
    dag)

#: Execution Rules

#: water_latest_only must run before get_indicator_bac_tests
get_indicator_bac_tests.set_upstream(wtr_latest_only)
#: Upload indicator bac tests after it has successfully run
upload_indicator_bac_tests.set_upstream(get_indicator_bac_tests)

#: get_last_bac_tests is dependent on get_indicator_bac_tests
get_latest_bac_tests.set_upstream(get_indicator_bac_tests)

#: Upload latest indicator bac tests after the file has been generated
upload_latest_indicator_bac_tests.set_upstream(get_latest_bac_tests)

#: update .md file after S3 upload
update_water_md.set_upstream(upload_latest_indicator_bac_tests)
