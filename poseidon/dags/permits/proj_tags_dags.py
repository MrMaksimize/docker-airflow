"""DSD Permits subdags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from dags.permits.proj_tags_jobs import *
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date
conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

dag = DAG(
  dag_id='dsd_proj_tags',
  default_args=args,
  start_date=start_date,
  schedule_interval=schedule,
  catchup=False
)

get_file = PythonOperator(
    task_id='get_tags_files',
    provide_context=True,
    python_callable=get_tags_file,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

create_prod = PythonOperator(
  task_id="create_tags_prod",
  provide_context=True,
  python_callable=build_tags,
  on_failure_callback=notify,
  on_retry_callback=notify,
  on_success_callback=notify,
  dag=dag)

upload_file = S3FileTransferOperator(
  task_id="upload_tags",
  source_base_path=conf['prod_data_dir'],
  source_key="permits_set1_project_tags_datasd.csv",
  dest_s3_bucket=conf['dest_s3_bucket'],
  dest_s3_conn_id=conf['default_s3_conn_id'],
  dest_s3_key="dsd/permits_set1_project_tags_datasd.csv",
  replace=True,
  on_failure_callback=notify,
  on_retry_callback=notify,
  on_success_callback=notify,
  dag=dag)

update_tags_md = get_seaboard_update_dag('project-tags-dsd.md', dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'development-permits-tags'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execution rules
get_file>>create_prod>>upload_file>>[update_tags_md,update_json_date]