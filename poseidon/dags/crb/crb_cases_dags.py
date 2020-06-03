"""CRB _dags file."""

from airflow.models import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

from dags.crb.crb_cases_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['crb']
start_date = general.start_date['crb']

#: Dag spec
dag = DAG(dag_id='crb_cases',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date)

crb_latest_only = LatestOnlyOperator(task_id='crb_latest_only', dag=dag)

#: Get the CRB cases Excel file from shared drive
get_crb_excel = PythonOperator(
    task_id='get_crb_excel',
    python_callable=get_crb_excel,
    dag=dag)

#: Process prod file
create_crb_cases_prod = PythonOperator(
    task_id='create_crb_cases_prod',
    python_callable=create_crb_cases_prod,
    dag=dag)

#: Upload prod file to S3
crb_upload = S3FileTransferOperator(
    task_id='upload_crb_cases',
    source_base_path=conf['prod_data_dir'],
    source_key=f'crb_cases_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key=f'crb/crb_cases_datasd.csv',
    replace=True,
    dag=dag)
    
#: Update dataset page on Seaboard
crb_md_update = get_seaboard_update_dag('crb_cases.md', dag)

#: Execution rules
#: latest_only must run before anything else
get_crb_excel.set_upstream(crb_latest_only)
#: get_crb_excel must run before create_crb_prod
create_crb_cases_prod.set_upstream(get_crb_excel)
#: create_crb_prod must run before crb_upload
crb_upload.set_upstream(create_crb_cases_prod)
#: update md must run after crb upload
crb_md_update.set_upstream(crb_upload)

