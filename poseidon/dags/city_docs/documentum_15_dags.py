""" Documentum web tables with daily schedule update _dags file"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email

import dags.city_docs.documentum_name as dn

from dags.city_docs.city_docs_jobs import *
from dags.city_docs.city_docs_subdags import *

args = general.args
conf = general.config
schedule = general.schedule['documentum_hr_15']
start_date = general.start_date['documentum_hr_15']

#: Dag spec
dag = DAG(dag_id='documentum_hourly_15', 
    default_args=args, 
    start_date=start_date, 
    schedule_interval=schedule,
    catchup=False)

schedule_mode = 'schedule_hourly_15'

#: Get documentum tables
get_doc_tables = PythonOperator(
    task_id='get_documentum_tables',
    python_callable=get_documentum,
    op_kwargs={'mode': schedule_mode,
    'test':False,
    'conn_id':'DOCM_SQL'},
    dag=dag)

upload_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(dn.table_name(schedule_mode),
    'documentum_hourly_15',
    False),
  dag=dag,
  )

#: Execution rules
get_doc_tables >> upload_files

