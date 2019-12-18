"""AMCS _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from dags.amcs.amcs_jobs import *
from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag


args = general.args
conf = general.config
# use get it done until I figure out what this means
schedule = general.schedule['get_it_done']
start_date = general.start_date['get_it_done']


dag = DAG(
    dag_id='amcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    user_defined_macros={
        'last_dag_run_execution_date': get_last_dag_run
    }
)


#: Latest Only Operator for amcs 
amcs_latest_only = LatestOnlyOperator(
    task_id='amcs_latest_only', dag=dag)


#: Builds the prod file
get_sites = PythonOperator(
    task_id='get_sites',
    python_callable=get_sites,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag
)

#: Counts the containers
group_site_containers = PythonOperator(
    task_id='group_site_containers',
    python_callable=group_site_containers,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag
)


#: Adds all the columns for the export
add_all_columns = PythonOperator(
    task_id='add_all_columns',
    python_callable=add_all_columns,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag
)

#: Compares the file to last file and stores only the updates
get_updates_only = PythonOperator(
    task_id='get_updates_only',
    python_callable=get_updates_only,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag
)


#: Writes the file to the shared drive
write_to_shared_drive = PythonOperator(
    task_id='write_to_shared_drive',
    python_callable=write_to_shared_drive,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag
)

#: Execution Rules

#: amcs_latest_only must run before get_sites
get_sites.set_upstream(amcs_latest_only)

#: get_sites must run before get_updates_only
get_updates_only.set_upstream(get_sites)

#: get_updates_only must run before group_site_containers
group_site_containers.set_upstream(get_updates_only)

#: group_site_containers must run before add_all_columns 
add_all_columns.set_upstream(group_site_containers)

#: add_all_columns must be run before write_to_shared_drive 
write_to_shared_drive.set_upstream(add_all_columns)
