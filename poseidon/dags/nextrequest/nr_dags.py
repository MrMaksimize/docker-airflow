from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general

from dags.nextrequest.nr_jobs import *

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = general.schedule['nextrequest']
start_date = general.start_date['nextrequest']
conf = general.config

dag = DAG(
    dag_id='nextrequest',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Downloads nextrequest data from API
get_nr_data_write_temp = PythonOperator(
    task_id='request_pra_date',
    python_callable=request_pra_date,
    provide_context=True,
    dag=dag)

#: Joins downloaded files from API to production
update_pv_prod = PythonOperator(
    task_id='update_prod',
    python_callable=update_prod,
    provide_context=True,
    dag=dag)

#TODO S3 Upload
'''
#: Uploads the pv production file
s3_upload = S3FileTransferOperator( # creating a different upload object for each...
    task_id='s3_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='pv_production.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key=f'pv_production/pv_production.csv',
    replace=True,
    dag=dag)
'''

#TODO execution rules
#: Update portal modified date
#update_pv_md = get_seaboard_update_dag('pv_production.md', dag)

#: Execution rules
#get_pv_data_write_temp >> [update_pv_prod,get_lucid_token]
#update_pv_prod >> check_upload_time >> s3_upload >> update_pv_md
#get_lucid_token >> push_lucid_data