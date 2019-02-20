"""DSD Permits _dags file."""
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from poseidon.util import general
from poseidon.dags.permits.permits_jobs import *
from poseidon.util.notifications import notify
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
year = general.get_year()

#: Dag spec for dsd permits
dag = DAG(dag_id='dsd_permits',
          default_args=args,
          schedule_interval=schedule)

#: Latest Only Operator for dsd permits.
dsd_permits_latest_only = LatestOnlyOperator(
    task_id='dsd_permits_latest_only', dag=dag)

#: Get permits reports
get_permits_files = PythonOperator(
    task_id='get_permits_files',
    python_callable=get_permits_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Clean permits reports
clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Upload data to S3
upload_dsd_permits = S3FileTransferOperator(
   task_id='upload_dsd_permits',
   source_base_path=conf['prod_data_dir'],
   source_key='dsd_permits_{}_datasd.csv'.format(year),
   dest_s3_bucket=conf['dest_s3_bucket'],
   dest_s3_conn_id=conf['default_s3_conn_id'],
   dest_s3_key='dsd/' + 'dsd_permits_{}_datasd.csv'.format(year),
   replace=True,
   on_failure_callback=notify,
   on_retry_callback=notify,
   on_success_callback=notify,
   dag=dag)

upload_solar_permits = S3FileTransferOperator(
   task_id='upload_solar_permits',
   source_base_path=conf['prod_data_dir'],
   source_key='solar_permits_{}_datasd.csv'.format(year),
   dest_s3_bucket=conf['dest_s3_bucket'],
   dest_s3_conn_id=conf['default_s3_conn_id'],
   dest_s3_key='dsd/' + 'solar_permits_{}_datasd.csv'.format(year),
   replace=True,
   on_failure_callback=notify,
   on_retry_callback=notify,
   on_success_callback=notify,
   dag=dag)



#: update permits.md file
update_permits_md = get_seaboard_update_dag('permits.md', dag)

#: update permits.md file
update_solar_md = get_seaboard_update_dag('solar-permits.md', dag)


#: Execution rules

#: dsd_permits_latest_only must run before get_permits_files
get_permits_files.set_upstream(dsd_permits_latest_only)

#: clean_data tasks are executed after get_approvals_files task
clean_data.set_upstream(get_permits_files)

#: upload_dsd tasks are executed after clean_data tasks
upload_dsd_permits.set_upstream(clean_data)

#: upload_dsd tasks are executed after clean_data tasks
upload_solar_permits.set_upstream(clean_data)

#: github updates are executed after S3 upload tasks
update_permits_md.set_upstream(upload_dsd_permits)

#: github updates are executed after S3 upload tasks
update_solar_md.set_upstream(upload_solar_permits)


