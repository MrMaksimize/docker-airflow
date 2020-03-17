""" Documentum web tables with one hour schedule update _dags file"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify
import dags.city_docs.documentum_name as dn

from dags.city_docs.city_docs_jobs import *
from datetime import datetime

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['documentum_hr_30']
start_date = general.start_date['documentum_hr_30']

#: Dag spec
dag = DAG(dag_id='documentum_hourly_30', catchup=False, default_args=args, start_date=start_date, schedule_interval=schedule)
prod_data = conf['prod_data_dir']
schedule_mode = 'schedule_hourly_30'

#: Get documentum tables
get_doc_tables = PythonOperator(
    task_id='get_documentum_tables',
    python_callable=get_documentum,
    op_kwargs={'mode': schedule_mode},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

div_doc_table = PythonOperator(
    task_id='divide_doc_table',
    python_callable=latest_res_ords,
    op_kwargs={'filename': 'documentum_scs_council_reso_ordinance_v'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

upload_reso_ord = S3FileTransferOperator(
    task_id='upload_documentum_reso_ordinance_latest',
    source_base_path=conf['prod_data_dir'],
    source_key='documentum_scs_council_reso_ordinance_v_2016_current.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='city_docs/documentum_scs_council_reso_ordinance_v_2016_current.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

get_doc_tables >> div_doc_table >> upload_reso_ord

files = [f for f in os.listdir(conf['prod_data_dir'])]
tables_other = dn.table_name(schedule_mode)
for f in files:
    file_name = f.split('.')[0]
    name_parts = file_name.split('_')
    if name_parts[0] == "documentum":
        file_check = '_'.join(name_parts[1:]).upper()
        if file_check in tables_other:
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
            upload_doc_tables << div_doc_table
