""" Sire web tables _dags file"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from dags.city_docs.city_docs_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['sire']
start_date = general.start_date['sire']

#: Dag spec
dag = DAG(dag_id='sire_docs', catchup=False, default_args=args, start_date=start_date, schedule_interval=schedule)

#: Get sire tables
get_doc_tables = PythonOperator(
    task_id='get_sire_tables',
    python_callable=get_sire,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

files = [f for f in os.listdir(conf['prod_data_dir'])]
for f in files:
    if f.split('_')[0] == "sire":
        #: Upload sire prod files to S3
        upload_doc_tables = S3FileTransferOperator(
            task_id='upload_{}'.format(f),
            source_base_path=conf['prod_data_dir'],
            source_key=f,
            dest_s3_conn_id=conf['default_s3_conn_id'],
            dest_s3_bucket=conf['dest_s3_bucket'],
            dest_s3_key='city_docs/{}'.format(f),
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            replace=True,
            dag=dag)


        #: get_doc_tables must run before upload_doc_tables
        upload_doc_tables.set_upstream(get_doc_tables)
