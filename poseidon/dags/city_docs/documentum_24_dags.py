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
schedule = general.schedule['documentum_daily']
start_date = general.start_date['documentum_daily']

div_file_pre = 'documentum_scs_council_reso_ordinance_v'

div_file_list = [f"{div_file_pre}_begin_1975",
f"{div_file_pre}_1976_1985",
f"{div_file_pre}_1986_1995",
f"{div_file_pre}_1996_2005",
f"{div_file_pre}_2006_2015",
f"{div_file_pre}_invalid"
]

#: Dag spec
dag = DAG(dag_id='documentum_daily', 
    catchup=False, 
    default_args=args, 
    start_date=start_date, 
    schedule_interval=schedule)

schedule_mode = 'schedule_daily'

#: Get documentum tables
get_doc_tables = PythonOperator(
    task_id='get_documentum_tables',
    python_callable=get_documentum,
    op_kwargs={'mode': schedule_mode,
    'test':False,
    'conn_id':'DOCM_SQL'},
    dag=dag)

div_doc_table = PythonOperator(
    task_id='divide_doc_other',
    python_callable=split_reso_ords,
    op_kwargs={'filename': 'documentum_scs_council_reso_ordinance_v'},
    dag=dag)

upload_div_files = SubDagOperator(
  task_id='upload_div_files',
  subdag=upload_div_files_subdag(div_file_list,
    'documentum_daily',
    False),
  dag=dag)

upload_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(dn.table_name(schedule_mode),
    'documentum_daily',
    False),
  dag=dag,
  )

#: Execution rules
get_doc_tables >> div_doc_table >> upload_div_files
get_doc_tables >> upload_files
