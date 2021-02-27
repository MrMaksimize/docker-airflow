"""inventory_dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.inventory.inv_jobs import *


conf = general.config

args = general.args

schedule = general.schedule['inventory']
start_date = general.start_date['inventory']

dag = DAG(dag_id='inventory',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Inventory Doc To CSV
inventory_to_csv = PythonOperator(
    task_id='inventory_to_csv',
    python_callable=inventory_to_csv,
    
    dag=dag)

#: Upload Inventory CSV to S3
upload_inventory = S3FileTransferOperator(
    task_id='upload_inventory',
    source_base_path=conf['prod_data_dir'],
    source_key='inventory_datasd_v1.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='inventory/inventory_datasd_v1.csv',
    
    replace=True,
    dag=dag)

#: Update portal modified date
update_inventory_md = get_seaboard_update_dag('data-inventory.md', dag)


#: Execution Rules
inventory_to_csv >> upload_inventory >> update_inventory_md
