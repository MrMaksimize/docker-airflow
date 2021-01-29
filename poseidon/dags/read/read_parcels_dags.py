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

dag = DAG(dag_id='read_parcels',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['read'],
    catchup=False
    )

#: Retrieve READ leases data from FTP
get_parcels = PythonOperator(
    task_id='get_parcels',
    python_callable=get_file,
    op_kwargs={'mode':'parcels'},
    dag=dag)

#: Process parcels data
process_parcels = PythonOperator(
    task_id='process_parcels',
    python_callable=process_parcels,
    dag=dag)

#: Upload parcels data to S3
parcels_to_S3 = S3FileTransferOperator(
    task_id='parcels_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='city_property_parcels_datasd_v1.csv',
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key='read/city_property_parcels_datasd_v1.csv',
    dag=dag)

update_json = PythonOperator(
    task_id=f"update_json_date_city_owned_property_parcels",
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'city_owned_property_parcels'},
    dag=dag)

#: Update parcels portal modified date
update_parcels_md = get_seaboard_update_dag('city-owned-properties-parcels.md', dag)

#: Execution Rules
get_parcels >> process_parcels >> parcels_to_S3 >> [update_json,update_parcels_md]
