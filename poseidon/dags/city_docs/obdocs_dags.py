""" OnBase web tables _dags file"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from dags.city_docs.city_docs_jobs import *
from datetime import datetime

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['onbase']
args['start_date'] = datetime.strptime('2018-11-12','%Y-%m-%d')

#: Dag spec
dag = DAG(dag_id='obdocs', default_args=args, schedule_interval=schedule)

onbase_docs_latest_only = LatestOnlyOperator(task_id='onbase_docs_latest_only', dag=dag)

#: Get onbase tables
get_doc_tables = PythonOperator(
    task_id='get_onbase_tables',
    python_callable=get_onbase,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execution rules
#: onbase_docs_latest_only must run before get_doc_tables
get_doc_tables.set_upstream(onbase_docs_latest_only)

files = [f for f in os.listdir(conf['prod_data_dir'])]
for f in files:
    file_name = f.split('.')[0]
    name_parts = file_name.split('_')
    if name_parts[0] == "onbase":
        #: Upload onbase prod files to S3
        upload_doc_tables = S3FileTransferOperator(
            task_id='upload_' + file_name,
            source_base_path=conf['prod_data_dir'],
            source_key='{}.csv'.format(file_name),
            dest_s3_conn_id=conf['default_s3_conn_id'],
            dest_s3_bucket=conf['dest_s3_bucket'],
            dest_s3_key='city_docs/{}.csv'.format(file_name),
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            replace=True,
            dag=dag)


        #: get_doc_tables must run before upload_doc_tables
        upload_doc_tables.set_upstream(get_doc_tables)
