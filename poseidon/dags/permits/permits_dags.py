"""DSD Permits _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from dags.permits.permits_jobs import *
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

#: Dag spec for dsd permits
dag = DAG(dag_id='dsd_permits',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule)

#: Get permits reports
get_permits_files = PythonOperator(
    task_id='get_permits_files',
    provide_context=True,
    python_callable=get_permits_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Clean permits reports
create_pts_active = PythonOperator(
    task_id='create_pts_active',
    provide_context=True,
    python_callable=build_pts,
    op_kwargs={'mode': 'active'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Join BIDs to permits
join_bids = PythonOperator(
    task_id='join_bids',
    python_callable=join_bids,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload data to S3
upload_dsd_permits = S3FileTransferOperator(
   task_id='upload_dsd_permits',
   source_base_path=conf['prod_data_dir'],
   source_key='dsd_permits_{}_datasd_v1.csv'.format(year),
   dest_s3_bucket=conf['dest_s3_bucket'],
   dest_s3_conn_id=conf['default_s3_conn_id'],
   dest_s3_key='dsd/' + 'dsd_permits_{}_datasd_v1.csv'.format(year),
   replace=True,
   on_failure_callback=notify,
   on_retry_callback=notify,
   on_success_callback=notify,
   dag=dag)


#: update permits.md file
update_permits_md = get_seaboard_update_dag('permits-dsd.md', dag)


#: Execution rules

#: dsd_permits_latest_only must run before get_permits_files
get_permits_files.set_upstream(dsd_permits_latest_only)

#: clean_data tasks are executed after get_approvals_files task
clean_data.set_upstream(get_permits_files)

#: upload_dsd tasks are executed after clean_data tasks
join_bids.set_upstream(clean_data)

#: upload_dsd tasks are executed after join bids tasks
upload_dsd_permits.set_upstream(join_bids)

#: github updates are executed after S3 upload tasks
update_permits_md.set_upstream(upload_dsd_permits)



