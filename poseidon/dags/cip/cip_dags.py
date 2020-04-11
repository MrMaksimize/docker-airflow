"""Capital_Improvements_Program_dags_file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.util.notifications import notify
from trident.util.seaboard_updates import *

from airflow.models import DAG
from trident.util import general
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

#: Latest Only Operator for CIP
cip_latest_only = LatestOnlyOperator(task_id='cip_latest_only', dag=dag)


#: Get CIP data from DB
get_cip_data = PythonOperator(
    task_id='get_cip_data',
    python_callable=get_cip_data,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod cip_datasd.csv file to S3
upload_cip_data = S3FileTransferOperator(
    task_id='upload_cip_data',
    source_base_path=conf['prod_data_dir'],
    source_key='cip_{0}_datasd_v1.csv'.format(fiscal_yr),
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='cip/' + 'cip_{0}_datasd_v1.csv'.format(fiscal_yr),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)


#: Update portal modified date
update_cip_md = get_seaboard_update_dag('cip.md', dag)

#: Execution order
#: cip_latest_only must run before get_cip_data
get_cip_data.set_upstream(cip_latest_only)

#: upload_cip_data is dependent on successful run of get_cip_data
upload_cip_data.set_upstream(get_cip_data)

#: upload_cip_data must succeed before updating github
update_cip_md.set_upstream(upload_cip_data)
