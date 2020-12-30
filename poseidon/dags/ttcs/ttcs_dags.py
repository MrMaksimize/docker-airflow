"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.ttcs.ttcs_jobs import *
from trident.util.seaboard_updates import *
import os
import glob

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['ttcs']


#: Dag definition
dag = DAG(dag_id='ttcs',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['ttcs'],
    catchup=False
    )

#: Get active businesses and save as .csv to temp folder
get_active_businesses = PythonOperator(
    task_id='get_active_businesses',
    python_callable=get_active_businesses,
    dag=dag)

#: Process temp data and save as .csv to prod folder
clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag)

#: Geocode new entries and update production file
geocode_data = PythonOperator(
    task_id='geocode_data',
    python_callable=geocode_data,
    dag=dag)

addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_address_book',
    source_base_path=conf['prod_data_dir'],
    source_key='ttcs_address_book.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_REF_BUCKET }}",
    dest_s3_key='reference/ttcs_address_book.csv',
    replace=True,
    dag=dag)

#: Spatially join BIDs data
join_bids = PythonOperator(
    task_id='join_bids',
    python_callable=join_bids,
    dag=dag)

#: Create subsets
create_subsets = PythonOperator(
    task_id='create_subsets',
    python_callable=make_prod_files,
    dag=dag)

#: Update portal modified date
update_ttcs_md = get_seaboard_update_dag('business-listings.md', dag)

#: Execution Rules
get_active_businesses >> clean_data >> geocode_data >> [addresses_to_S3,join_bids]
join_bids >> create_subsets

subset_names = [os.path.basename(x) for x in glob.glob(conf['prod_data_dir']+'/sd_businesses_*_datasd_v1.csv')]

for index, subset in enumerate(subset_names):

    file_name = subset.split('.')[0]
    name_parts = file_name.split('_')
    if 'v1' in name_parts:
        name_parts.remove('datasd')
        name_parts.remove('v1')
        task_name = '_'.join(name_parts[2:])

        #: Upload prod active file to S3
        active_to_S3 = S3FileTransferOperator(
            task_id=f'upload_{task_name}',
            source_base_path=conf['prod_data_dir'],
            source_key=f'sd_businesses_{task_name}_datasd_v1.csv',
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f'ttcs/sd_businesses_{task_name}_datasd_v1.csv',
            replace=True,
            dag=dag)

        #: make_operating must run after the get task
        create_subsets >> active_to_S3

        if index == len(subset_names)-1:

            active_to_S3 << update_ttcs_md