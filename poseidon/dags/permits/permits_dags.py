"""DSD Permits _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.permits.permits_jobs import *
from dags.permits.permits_subdags import *

from trident.util.seaboard_updates import *
conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']

#: Dag spec for dsd permits
dag = DAG(dag_id='dsd_permits',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule,
          catchup=False)

#: Join BIDs to 4 files using subdag
all_accela = SubDagOperator(
  task_id='get_create_accela',
  subdag=get_create_accela_subdag(),
  dag=dag)

#: Subset files
all_pts = SubDagOperator(
  task_id='get_create_pts',
  subdag=get_create_pts_subdag(),
  dag=dag)

exec_snowflake_pts = SubDagOperator(
  task_id="snowflake_pts",
  subdag=snowflake_pts_subdag(),
  dag=dag)

exec_snowflake_accela = SubDagOperator(
  task_id="snowflake_accela",
  subdag=snowflake_accela_subdag(),
  dag=dag)

#: Upload 4 files using subdag
upload_set1_files = SubDagOperator(
  task_id='upload_set1_files',
  subdag=upload_set1_files_subdag(),
  dag=dag)

#: Upload 4 files using subdag
upload_set2_files = SubDagOperator(
  task_id='upload_set2_files',
  subdag=upload_set2_files_subdag(),
  dag=dag)

update_set1_md = get_seaboard_update_dag('development-permits-set1.md', dag)
update_set2_md = get_seaboard_update_dag('development-permits-set2.md', dag)

#: Update data inventory json
update_set1_json_date = PythonOperator(
    task_id='update_set1_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'development-permits-set1'},
    dag=dag)

#: Update data inventory json
update_set2_json_date = PythonOperator(
    task_id='update_set2_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'development-permits-set2'},
    dag=dag)

#: Create TSW subset
create_tsw_file = PythonOperator(
    task_id='create_tsw',
    python_callable=create_tsw_subset,
    dag=dag)

#: Create PW subset
create_pw_sap_file = PythonOperator(
    task_id='create_pw_sap',
    python_callable=create_pw_sap_subset,
    dag=dag)

# Upload TSW subset
upload_tsw = S3FileTransferOperator(
    task_id="upload_tsw_subset",
    source_base_path=conf['prod_data_dir'],
    source_key=f"dsd_permits_row.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key=f"tsw/dsd_permits_row.csv",
    replace=True,
    dag=dag)

# Upload TSW subset
upload_pw_sap = S3FileTransferOperator(
    task_id="upload_pw_subset",
    source_base_path=conf['prod_data_dir'],
    source_key=f"dsd_permits_public_works.csv",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key=f"dsd/dsd_permits_public_works.csv",
    replace=True,
    dag=dag,
    )

#: Execution rules
all_accela>>exec_snowflake_accela
all_pts>>exec_snowflake_pts
all_accela>>upload_set2_files
all_pts>>upload_set1_files
upload_set1_files>>[update_set1_md,update_set1_json_date]
upload_set2_files>>[update_set2_md,update_set2_json_date]
all_accela>>[create_tsw_file,create_pw_sap_file]
all_pts>>[create_tsw_file,create_pw_sap_file]
create_tsw_file>>upload_tsw
create_pw_sap_file>>upload_pw_sap
