"""Fire_department_dags_file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.util.notifications import notify

from airflow.models import DAG
from trident.util import general
from dags.fd_incidents.fd_jobs import *
from trident.util.seaboard_updates import *



args = general.args
conf = general.config
schedule = general.schedule['fd_incidents']
start_date = general.start_date['fd_incidents']
cur_yr = general.get_year()

#: Dag spec
dag = DAG(dag_id='fd_incidents',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Latest Only Operator for fd
fd_latest_only = LatestOnlyOperator(task_id='fd_latest_only', dag=dag)


#: Get fire_department data from DB
get_fd_data = PythonOperator(
    task_id='get_fd_data',
    python_callable=get_fd_data,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod fire_department_SD.csv file to S3
upload_fd_data = S3FileTransferOperator(
    task_id='upload_fd_data',
    source_base_path=conf['prod_data_dir'],
    source_key='fd_incidents_{0}_datasd_v1.csv'.format(cur_yr),
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='fd_cad/' + 'fd_incidents_{0}_datasd_v1.csv'.format(cur_yr),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'fire_ems_incidents'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Update portal modified date
update_fire_department_incidents_md = get_seaboard_update_dag('fire-incidents.md', dag)

#: Execution order

#: fd_latest_only must run before get_fd_data
get_fd_data.set_upstream(fd_latest_only)

#: upload_fd_data is dependent on successful run of get_fd_data
upload_fd_data.set_upstream(get_fd_data)

#: upload_fd_data must succeed before updating github
update_fire_department_incidents_md.set_upstream(upload_fd_data)

#: upload_fd_data must succeed before updating json
update_json_date.set_upstream(upload_fd_data)
