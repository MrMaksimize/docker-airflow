"""READ _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.read.read_jobs import *

from trident.util.seaboard_updates import *

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['read']

dag = DAG(dag_id='read_billing',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['read'],
    catchup=False
    )

#: Retrieve READ billing data from FTP
get_billing = PythonOperator(
    task_id='get_billing',
    python_callable=get_file,
    op_kwargs={'mode':'billing'},
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Process billing data
process_billing = PythonOperator(
    task_id='process_billing',
    python_callable=process_billing,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Upload billing data to S3
billing_to_S3 = S3FileTransferOperator(
    task_id='billing_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='city_property_billing_datasd_v1.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/city_property_billing_datasd_v1.csv',
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Execution Rules
get_billing >> process_billing >> billing_to_S3
