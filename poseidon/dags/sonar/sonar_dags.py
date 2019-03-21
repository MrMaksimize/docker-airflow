"""Sonar _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.operators.poseidon_email_operator import PoseidonEmailWithPythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from dags.sonar.sonar_jobs import build_sonar_json, get_sonar_json

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['sonar']
start_date = general.start_date['sonar']

#: Dag spec
dag = DAG(dag_id='sonar', default_args=args, start_date=start_date, schedule_interval=schedule)

sonar_latest_only = LatestOnlyOperator(
    task_id='sonar_latest_only', dag=dag)

#: Build sonar json file
build_sonar_json = PythonOperator(
    task_id='build_sonar_json',
    python_callable=build_sonar_json,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

send_sonar_email_default = PoseidonEmailWithPythonOperator(
    task_id='send_sonar_email_default',
    to='maksimp@sandiego.gov',
    subject='Your Sonar',
    template_id='tem_Hr9qDhgyTMPM4Kj9yDkFyCHd',
    dispatch_type='sonar_dispatch',
    python_callable=get_sonar_json,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)



#: Execution rules
#: sonar_latest_only must run before build_sonar_json
build_sonar_json.set_upstream(sonar_latest_only)
#: build_sonar_json runs before send_sonar_email
send_sonar_email_default.set_upstream(build_sonar_json)
