"""Fire_department_dags_file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator


from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.fd_problem_nature.fd_pn_jobs import *
from trident.util.seaboard_updates import *

args = general.args
conf = general.config
schedule = general.schedule['fd_incidents']
start_date = general.start_date['fd_incidents']
cur_yr = general.get_year()

#: Dag spec
dag = DAG(dag_id='fd_problem_nature',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )


#: Get fire_department data from DB
get_fd_data = PythonOperator(
    task_id='get_fd_data',
    python_callable=get_fd_data,
    provide_context=True,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Upload prod fire_department_SD.csv file to S3
upload_fd_data = S3FileTransferOperator(
    task_id='upload_fd_data',
    source_base_path=conf['prod_data_dir'],
    source_key='fd_problem_nature_agg_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='fd_cad/fd_problem_nature_agg_datasd_v1.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'fire_ems_problem_counts'},
    on_failure_callback=afsys_send_email,
    dag=dag)

update_fire_incidents_problems = get_seaboard_update_dag('fire-incident-problem-agg.md', dag)

#: Execution order
get_fd_data >> upload_fd_data >> [update_fire_incidents_problems,update_json_date]
