"""Public art _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email


#from dags.public_art.public_art_jobs import *
from dags.public_art.public_art_jobs import *
from trident.util.seaboard_updates import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['public_art']
start_date = general.start_date['public_art']

#: Dag spec
dag = DAG(dag_id='public_art',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Get public art from NetX, process, output prod file
get_public_art = PythonOperator(
    task_id='get_public_art',
    python_callable=get_public_art,
    
    dag=dag)

process_public_art = PythonOperator(
    task_id='process_public_art',
    python_callable=process_public_art,
    
    dag=dag)

#: Upload prod art file to S3
upload_public_art = S3FileTransferOperator(
    task_id='upload_public_art',
    source_base_path=conf['prod_data_dir'],
    source_key='public_art_locations_datasd_v1.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='public_art/public_art_locations_datasd_v1.csv',
    
    replace=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'civic_art_collection'},
    
    dag=dag)

#: Update portal modified date
update_public_art_md = get_seaboard_update_dag('public-art.md', dag)


#: Execution rules
get_public_art >> process_public_art >> upload_public_art >> [update_public_art_md,update_json_date]
