"""DSD Permits _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from trident.util import general
from dags.permits.permits_jobs import *
from dags.permits.permits_subdags import *
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date
conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

#: Dag spec for dsd permits
dag = DAG(dag_id='dsd_permits',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule,
          catchup=False
          )

#: Get permits reports
get_permits_files = PythonOperator(
    task_id='get_permits_files',
    provide_context=True,
    python_callable=get_permits_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Create 4 files using subdag
create_files = SubDagOperator(
  task_id='create_files',
  subdag=create_file_subdag(),
  dag=dag,
  )

#: Join BIDs to 4 files using subdag
join_bids = SubDagOperator(
  task_id='join_bids',
  subdag=join_bids_subdag(),
  dag=dag,
  )

#: Create full sets for internal
create_full = PythonOperator(
    task_id='create_full_sets',
    python_callable=create_full_set,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload 4 files using subdag
upload_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(),
  dag=dag,
  )

update_set1_md = get_seaboard_update_dag('development-permits-set1.md', dag)
update_set2_md = get_seaboard_update_dag('development-permits-set2.md', dag)

#: Update data inventory json
update_set1_json_date = PythonOperator(
    task_id='update_set1_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'development-permits-set1'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update data inventory json
update_set2_json_date = PythonOperator(
    task_id='update_set2_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'development-permits-set2'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Create TSW subset
create_tsw_file = PythonOperator(
    task_id='create_tsw',
    python_callable=create_tsw_subset,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Create PW subset
create_pw_sap_file = PythonOperator(
    task_id='create_pw_sap',
    python_callable=create_pw_sap_subset,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

# Upload TSW subset
upload_tsw = S3FileTransferOperator(
      task_id="upload_tsw_subset",
      source_base_path=conf['prod_data_dir'],
      source_key=f"dsd_permits_row.csv",
      dest_s3_bucket=conf['dest_s3_bucket'],
      dest_s3_conn_id=conf['default_s3_conn_id'],
      dest_s3_key=f"tsw/dsd_permits_row.csv",
      replace=True,
      on_failure_callback=notify,
      on_retry_callback=notify,
      on_success_callback=notify,
      dag=dag,
    )

# Upload TSW subset
upload_pw_sap = S3FileTransferOperator(
      task_id="upload_pw_subset",
      source_base_path=conf['prod_data_dir'],
      source_key=f"dsd_permits_public_works.csv",
      dest_s3_bucket=conf['dest_s3_bucket'],
      dest_s3_conn_id=conf['default_s3_conn_id'],
      dest_s3_key=f"dsd/dsd_permits_public_works.csv",
      replace=True,
      on_failure_callback=notify,
      on_retry_callback=notify,
      on_success_callback=notify,
      dag=dag,
    )

#: Execution rules
get_permits_files>>create_files>>join_bids>>create_full>>upload_files
upload_files>>[update_set1_md,update_set2_md,update_set1_json_date,update_set2_json_date]
upload_files>>[create_tsw_file,create_pw_sap_file]
create_tsw_file>>upload_tsw
create_pw_sap_file>>upload_pw_sap
