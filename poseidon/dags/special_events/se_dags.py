"""Special Events _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.special_events.se_jobs import *
from trident.util.seaboard_updates import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['special_events']
start_date = general.start_date['special_events']

#: Dag spec
dag = DAG(dag_id='special_events',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule,
          catchup=False)


#: Get special events from DB
get_special_events = PythonOperator(
    task_id='get_special_events',
    python_callable=get_special_events,
    
    dag=dag)

#: Process and geocode raw special events file
process_special_events = PythonOperator(
    task_id='process_special_events',
    python_callable=process_special_events,
    
    dag=dag)

#: Process and geocode raw special events file
addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_address_book',
    source_base_path=conf['prod_data_dir'],
    source_key='events_address_book.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/events_address_book.csv',
    
    replace=True,
    dag=dag)

#: Upload prod SE file to S3
upload_special_events_web = S3FileTransferOperator(
    task_id='upload_special_events_web',
    source_base_path=conf['prod_data_dir'],
    source_key='special_events_list_datasd.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='special_events/special_events_list_datasd.csv',
    
    replace=True,
    dag=dag)

#: Upload prod SE file to S3
upload_special_events = S3FileTransferOperator(
    task_id='upload_special_events',
    source_base_path=conf['prod_data_dir'],
    source_key='special_events_list_datasd_v1.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='special_events/special_events_list_datasd_v1.csv',
    
    replace=True,
    dag=dag)

update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'special_events'},
    
    dag=dag)

#: Update portal modified date
update_special_events_md = get_seaboard_update_dag('special-events.md', dag)

#: Execution rules
get_special_events >> process_special_events
process_special_events >> [addresses_to_S3,upload_special_events,upload_special_events_web]
upload_special_events >> [update_json_date,update_special_events_md]