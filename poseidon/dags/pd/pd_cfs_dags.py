"""Police calls_for_service _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from dags.pd.pd_cfs_jobs import *
from trident.util import general
from trident.util.notifications import afsys_send_email

from trident.util.seaboard_updates import *
import os

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['pd_cfs']

dag = DAG(
    dag_id='pd_cfs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['pd_cfs'],
    catchup=False
    )


#: Get CFS data from FTP and save to temp folder
get_cfs_data = PythonOperator(
    task_id='get_cfs_data',
    python_callable=get_cfs_data,
    provide_context=True,
    dag=dag)

#: Process CFS data and save result to prod folder
process_cfs_data = PythonOperator(
    task_id='process_cfs_data',
    python_callable=process_cfs_data,
    provide_context=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'police_calls_for_service'},
    
    dag=dag)

#: Update portal modified date
update_pd_cfs_md = get_seaboard_update_dag('police-calls-for-service.md', dag)

get_cfs_data >> process_cfs_data

run_date = general.get_last_run(dag)
#run_date = pendulum.parse('2020-01-20T23:00:00+00:00')

filename = f"{conf['prod_data_dir']}/pd_calls_for_service_*_datasd.csv"
files = [x for x in glob.glob(filename)]
updated = [u for u in files if pendulum.from_timestamp(os.path.getmtime(u)) >= run_date]


for index, file in enumerate(updated):

    file_year = [int(s) for s in file.split('_') if s.isdigit()]

    #: Upload prod file to S3
    cfs_to_S3 = S3FileTransferOperator(
        task_id=f'cfs_{file_year[0]}_to_S3',
        source_base_path=conf['prod_data_dir'],
        source_key=f'pd_calls_for_service_{file_year[0]}_datasd.csv',
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_key=f'pd/pd_calls_for_service_{file_year[0]}_datasd.csv',
        
        dag=dag)

    cfs_to_S3 << process_cfs_data

    if index == len(updated)-1:

        [update_pd_cfs_md,update_json_date] << cfs_to_S3
