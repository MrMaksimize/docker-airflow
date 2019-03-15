"""Dag configuration for dsd."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

from datetime import datetime, timedelta
from trident.util import general

from dags.dsd.dsd_jobs import get_bash_command
from dags.dsd import dsdFileGetter as dfg

from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from trident.util.notifications import notify

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.schedule['dsd_code_enforcement']

fname_list = ['code_enf_past_6_mo', 'code_enf_past_3_yr']

dsd_temp_dir = general.create_path_if_not_exists(conf['temp_data_dir'] + '/')

#: Dag spec for dsd code enforcement.
dag = DAG(dag_id='dsd_code_enforcement',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule['dsd_code_enforcement'])

#: Latest Only Operator for dsd code enforcement
dsd_ce_latest_only = LatestOnlyOperator(
    task_id='dsd_code_enf_latest_only', dag=dag)


#: Download code enforcement files and unzip them.
get_code_enf_files = PythonOperator(
    task_id='get_code_enf_files',
    python_callable=dfg.get_files,
    op_kwargs={'fname_list': fname_list,
               'target_dir': dsd_temp_dir},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_code_enf_md = get_seaboard_update_dag('code-enforcement-violations.md', dag)

#: Execution rules
#: dsd_code_enf_latest_only must run before get_code_enf_files
get_code_enf_files.set_upstream(dsd_ce_latest_only)


for i in fname_list:
    #: Create fme shell command
    build_csv_task = BashOperator(
        task_id='get_' + i,
        bash_command=get_bash_command(i),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Set Task as Downstream for downloading files
    build_csv_task.set_upstream(get_code_enf_files)

    #: Create S3 Upload task
    s3_task = S3FileTransferOperator(
        task_id='upload_' + i,
        source_base_path=conf['prod_data_dir'],
        source_key=i + '_datasd.csv',
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_key='dsd/' + i + '_datasd.csv',
        replace=True,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Set S3 Task as downstream for file task.
    s3_task.set_upstream(build_csv_task)


    #: Create S3 Upload task
    s3_cmpl_task = S3FileTransferOperator(
        task_id='upload_cmpl_' + i,
        source_base_path=conf['prod_data_dir'],
        source_key=i + '_complaints_datasd.csv',
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_key='dsd/' + i + '_complaints_datasd.csv',
        replace=True,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Set S3 Task as downstream for file task.
    s3_cmpl_task.set_upstream(build_csv_task)


    #: data upload must succeed before updating github
    update_code_enf_md.set_upstream(s3_task)
    update_code_enf_md.set_upstream(s3_cmpl_task)
