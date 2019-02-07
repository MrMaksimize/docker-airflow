"""waze_dags file."""
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from poseidon.util import general
from poseidon.util.notifications import notify
from poseidon.dags.waze.waze_jobs import *
from datetime import datetime, timedelta


conf = general.config

args = general.args

schedule = general.schedule['waze']

dag = DAG(dag_id='waze', default_args=args, schedule_interval=schedule)

#: Latest Only Operator for jams
jams_latest_only = LatestOnlyOperator(task_id='waze_jams_latest_only', dag=dag)

#: Waze feed to csv
waze_jams_to_csv = PythonOperator(
    task_id='waze_jams_to_csv',
    python_callable=waze_jams_to_csv,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Retreive traffic jams from Waze feed
waze_jams_to_db = PythonOperator(
    task_id='waze_jams_to_db',
    python_callable=waze_jams_to_db,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Execution Rules

#: jams_latest_only must run before waze_jams_to_csv
waze_jams_to_csv.set_upstream(jams_latest_only)

#: jams_latest_only must run before waze_jams_to_db
waze_jams_to_db.set_upstream(waze_jams_to_csv)
