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

dag = DAG(dag_id='read_properties',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['read'],
    catchup=False
    )

#: Retrieve READ leases data from FTP
get_properties_details = PythonOperator(
    task_id='get_properties',
    python_callable=get_file,
    op_kwargs={'mode':'properties'},
    dag=dag)

#: Process properties details data
process_properties_details = PythonOperator(
    task_id='process_properties_details',
    python_callable=process_properties_details,
    dag=dag)

#: Upload properties details data to S3
properties_details_to_S3 = S3FileTransferOperator(
    task_id='properties_details_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='city_property_details_datasd_v1.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/city_property_details_datasd_v1.csv',
    dag=dag)

update_json = PythonOperator(
    task_id=f"update_json_date_city_owned_properties",
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'city_owned_properties'},
    dag=dag)

#: Update leases portal modified date
update_leases_md = get_seaboard_update_dag('city-owned-properties-details.md', dag)

#: Execution Rules

get_properties_details >> process_properties_details >> properties_details_to_S3 >> [update_json,update_leases_md]
