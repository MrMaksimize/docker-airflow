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

dag = DAG(dag_id='read_leases',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['read'],
    catchup=False
    )

#: Retrieve READ leases data from FTP
get_leases = PythonOperator(
    task_id='get_leases',
    python_callable=get_file,
    op_kwargs={'mode':'leases'},
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Process leases data
process_leases = PythonOperator(
    task_id='process_leases',
    python_callable=process_leases,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Upload leases data to S3
leases_to_S3 = S3FileTransferOperator(
    task_id='leases_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='city_property_leases_datasd_v1.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/city_property_leases_datasd_v1.csv',
    on_failure_callback=afsys_send_email,
    dag=dag)

update_json = PythonOperator(
    task_id=f"update_json_date_leases_city_owned_properties",
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'leases_city_owned_properties'},
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Update leases portal modified date
update_leases_md = get_seaboard_update_dag('city-owned-properties-leases.md', dag)


#: Execution Rules
get_leases >> process_leases >> leases_to_S3 >> [update_json,update_leases_md]
