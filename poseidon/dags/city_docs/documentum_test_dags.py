""" Documentum web tables with one hour schedule update _dags file"""
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
from datetime import datetime

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['documentum_hr_30']
start_date = general.start_date['documentum_hr_30']

div_file_pre = 'documentum_scs_council_reso_ordinance_v_test'

div_file_list = [f"{div_file_pre}_begin_1975",
f"{div_file_pre}_1976_1985",
f"{div_file_pre}_1986_1995",
f"{div_file_pre}_1996_2005",
f"{div_file_pre}_2006_2015",
f"{div_file_pre}_2016_current",
f"{div_file_pre}_invalid"
]

other_file_list = dn.table_name('schedule_daily')+dn.table_name('schedule_hourly_15')+dn.table_name('schedule_hourly_30')

#: Dag spec
dag = DAG(dag_id='documentum_test',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

schedule_mode = 'schedule_hourly_30'

#: Get documentum tables
get_doc_tables = PythonOperator(
    task_id='get_documentum_tables',
    python_callable=get_documentum,
    op_kwargs={'mode': schedule_mode,
    'test':True,
    'conn_id':'docm_test_sql'},
    dag=dag)

div_doc_latest = PythonOperator(
    task_id='divide_doc_latest',
    python_callable=latest_res_ords,
    op_kwargs={'filename': 'documentum_scs_council_reso_ordinance_v_test'},
    dag=dag)

div_doc_other = PythonOperator(
    task_id='divide_doc_other',
    python_callable=split_reso_ords,
    op_kwargs={'filename': 'documentum_scs_council_reso_ordinance_v_test'},
    dag=dag)

upload_div_files = SubDagOperator(
  task_id='upload_div_files',
  subdag=upload_div_files_subdag(div_file_list,
    'documentum_test',
    True),
  dag=dag)

upload_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(other_file_list,
    'documentum_test',
    True),
  dag=dag)

#: Execution rules
get_doc_tables >> div_doc_latest >> div_doc_other >> upload_div_files
get_doc_tables >> upload_files