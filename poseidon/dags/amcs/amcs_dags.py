"""AMCS _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from dags.amcs.amcs_jobs import *
from trident.util import general

args = general.args
conf = general.config
schedule = general.schedule['amcs']
start_date = general.start_date['amcs']


dag = DAG(
    dag_id='amcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
)


#: Builds the prod file
get_sites = PythonOperator(
    task_id='get_sites',
    python_callable=get_sites,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag
)

#: Counts the containers
group_site_containers = PythonOperator(
    task_id='group_site_containers',
    python_callable=group_site_containers,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag
)


#: Adds all the columns for the export
add_all_columns = PythonOperator(
    task_id='add_all_columns',
    python_callable=add_all_columns,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag
)

#: Compares the file to last file and stores only the updates
get_updates_only = PythonOperator(
    task_id='get_updates_only',
    python_callable=get_updates_only,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag
)


#: Writes the file to the shared drive
write_to_shared_drive = PythonOperator(
    task_id='write_to_shared_drive',
    python_callable=write_to_shared_drive,
    email=['data@sandiego.gov'],
    email_on_failure=True,
    dag=dag
)

#: Execution Rules
get_sites >> get_updates_only >> group_site_containers >> add_all_columns >> write_to_shared_drive
