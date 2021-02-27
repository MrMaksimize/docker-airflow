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

#: Dag spec
dag = DAG(dag_id='documentum_hourly_30',
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
    'test':False,
    'conn_id':'DOCM_SQL'},
    dag=dag)

#div_doc_table = PythonOperator(
    #task_id='divide_doc_latest',
    #python_callable=latest_res_ords,
    #op_kwargs={'filename': 'documentum_scs_council_reso_ordinance_v'},
    #dag=dag)

#upload_reso_ord = S3FileTransferOperator(
    #task_id='upload_documentum_reso_ordinance_latest',
    #source_base_path=conf['prod_data_dir'],
    #source_key='documentum_scs_council_reso_ordinance_v_2016_current.csv',
    #dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    #dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    #dest_s3_key='city_docs/documentum_scs_council_reso_ordinance_v_2016_current.csv',
    #replace=True,
    #dag=dag)

upload_files = SubDagOperator(
  task_id='upload_files',
  subdag=upload_files_subdag(dn.table_name(schedule_mode),
  	'documentum_hourly_30',
  	False),
  dag=dag)

#get_doc_tables >> div_doc_table >> upload_reso_ord
get_doc_tables >> upload_files
