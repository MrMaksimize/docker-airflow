"""inventory_dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import notify
from dags.inventory.inv_jobs import *


conf = general.config

args = general.args

schedule = general.schedule['inventory']
start_date = general.start_date['inventory']

dag = DAG(dag_id='inventory', default_args=args, start_date=start_date, schedule_interval=schedule)


inv_latest_only = LatestOnlyOperator(task_id='inventory_latest_only', dag=dag)


#: Inventory Doc To CSV
inventory_to_csv = PythonOperator(
    task_id='inventory_to_csv',
    python_callable=inventory_to_csv,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload Inventory CSV to S3
upload_inventory = S3FileTransferOperator(
    task_id='upload_inventory',
    source_base_path=conf['prod_data_dir'],
    source_key='inventory_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='inventory/inventory_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)



#: Update portal modified date
update_inventory_md = get_seaboard_update_dag('data-inventory.md', dag)


#: Execution Rules
#: Latest only for inventory to csv
inventory_to_csv.set_upstream(inv_latest_only)
#: Inventory csv gets created before its uploaded
upload_inventory.set_upstream(inventory_to_csv)

#: upload_gid_requests must succeed before updating github
update_inventory_md.set_upstream(upload_inventory)
