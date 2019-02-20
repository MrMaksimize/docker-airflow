"""This module contains dags and tasks for extracting data out of TTCS."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import notify
from dags.ttcs.ttcs_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag
import os
import glob

args = general.args
conf = general.config
schedule = general.schedule

#: Dag definition
dag = DAG(dag_id='ttcs', default_args=args, schedule_interval=schedule['ttcs'])


#: Latest Only Operator for ttcs
ttcs_latest_only = LatestOnlyOperator(
    task_id='ttcs_latest_only', dag=dag)

#: Get active businesses and save as .csv to temp folder
get_active_businesses = PythonOperator(
    task_id='get_active_businesses',
    python_callable=get_active_businesses,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process temp data and save as .csv to prod folder
clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Geocode new entries and update production file
geocode_data = PythonOperator(
    task_id='geocode_data',
    python_callable=geocode_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Spatially join BIDs data
join_bids = PythonOperator(
    task_id='join_bids',
    python_callable=join_bids,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Create subsets
create_subsets = PythonOperator(
    task_id='create_subsets',
    python_callable=make_prod_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_ttcs_md = get_seaboard_update_dag('business-listings.md', dag)

#: Execution Rules

#: ttcs_latest_only must run before get_active
get_active_businesses.set_upstream(ttcs_latest_only)
#: ttcs_latest_only must run before get_bids
clean_data.set_upstream(get_active_businesses)
#: Data cleaning occurs after BIDs data retrieval.
geocode_data.set_upstream(clean_data)
#: spatial join occurs after geocoding.
join_bids.set_upstream(geocode_data)
#: last 3mo subsetting occurs after spatial join
create_subsets.set_upstream(join_bids)

subset_names = [os.path.basename(x) for x in glob.glob(conf['prod_data_dir']+'/sd_businesses_*.csv')]

for index, subset in enumerate(subset_names):

    fname = subset[14:-11]

    #: Upload prod active file to S3
    active_to_S3 = S3FileTransferOperator(
        task_id='upload_'+fname,
        source_base_path=conf['prod_data_dir'],
        source_key='sd_businesses_'+fname+'_datasd.csv',
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='ttcs/sd_businesses_'+fname+'_datasd.csv',
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)

    #: make_operating must run after the get task
    active_to_S3.set_upstream(create_subsets)

    if index == len(subset_names)-1:

        active_to_S3.set_downstream(update_ttcs_md)
