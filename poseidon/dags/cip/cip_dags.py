"""Capital_Improvements_Program_dags_file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from trident.util.seaboard_updates import *

from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.cip.cip_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['cip']
start_date = general.start_date['cip']
fiscal_yr = general.get_FY_year()

#: Dag spec
dag = DAG(dag_id='cip',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Get CIP data from DB
get_cip_data = PythonOperator(
    task_id='get_cip_data',
    python_callable=get_cip_data,
    provide_context=True,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Upload prod cip_datasd.csv file to S3
upload_cip_data = S3FileTransferOperator(
    task_id='upload_cip_data',
    source_base_path=conf['prod_data_dir'],
    source_key=f'cip_{fiscal_yr}_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key=f'cip/cip_{fiscal_yr}_datasd_v1.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)


#: Execution order
get_cip_data >> upload_cip_data