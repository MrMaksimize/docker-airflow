"""FM budget _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from dags.budget.budget_jobs import *
from dags.budget.actuals_jobs import *
from dags.budget.budget_subdags import *
from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag


args = general.args
conf = general.config
schedule = general.schedule['budget']
start_date = general.start_date['budget']

dag = DAG(
    dag_id='budget',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

get_accounts = PythonOperator(
    task_id='get_chart_of_accounts',
    python_callable=get_accounts_chart,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_files = SubDagOperator(
    task_id='get_files',
    subdag=get_files_subdag(),
    dag=dag)

get_refs = PythonOperator(
    task_id='get_reference_sets',
    python_callable=get_ref_sets,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

create_files = SubDagOperator(
    task_id='create_files',
    subdag=create_files_subdag(),
    dag=dag)

upload_files = SubDagOperator(
    task_id='upload_files',
    subdag=upload_files_subdag(),
    dag=dag)

upload_refs = SubDagOperator(
    task_id='upload_refs',
    subdag=upload_ref_files_subdag(),
    dag=dag)

#: Execution Rules

get_accounts >> get_refs >> get_files
get_refs >> upload_refs
get_files >> create_files >> upload_files
