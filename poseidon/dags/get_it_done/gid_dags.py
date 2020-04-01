"""Get It Done _dags file."""
import re
import glob
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator

from trident.util import general
from trident.util.notifications import notify

from trident.util.seaboard_updates import *

from dags.get_it_done.gid_jobs import *
from dags.get_it_done.gid_subdags import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['get_it_done']
start_date = general.start_date['get_it_done']

#: Dag spec
dag = DAG(dag_id='get_it_done',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Get GID requests from Salesforce
get_streets_requests = PythonOperator(
    task_id='get_gid_streets',
    python_callable=get_gid_streets,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get GID requests from Salesforce
get_other_requests = PythonOperator(
    task_id='get_gid_other',
    python_callable=get_gid_other,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Create mapped case record type and service name cols
update_service_name = PythonOperator(
    task_id='update_service_name',
    python_callable=update_service_name,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Fix close dates per SAP issue
update_close_dates = PythonOperator(
    task_id='update_close_dates',
    python_callable=update_close_dates,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Fix referral column
update_referral_col = PythonOperator(
    task_id='update_referral_col',
    python_callable=update_referral_col,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

spatial_joins = SubDagOperator(
  task_id='spatial_joins',
  subdag=spatial_join_subdag(),
  dag=dag,
  )

service_names = SubDagOperator(
  task_id='service_names',
  subdag=service_name_subdag(),
  dag=dag,
  )

#: Divide records by year for prod files
create_prod_files = PythonOperator(
    task_id='create_prod_files',
    python_callable=create_prod_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

upload_prod_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(),
  dag=dag,
  )

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'get_it_done_reports'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

md_update_task = get_seaboard_update_dag('get-it-done-311.md', dag)
            
#: Execution rules
[get_streets_requests, get_other_requests] >> update_service_name
update_service_name >> update_close_dates
update_close_dates >> update_referral_col
update_referral_col >> spatial_joins
spatial_joins >> create_prod_files >> [service_names, upload_prod_files]
upload_prod_files >> [md_update_task,update_json_date]
