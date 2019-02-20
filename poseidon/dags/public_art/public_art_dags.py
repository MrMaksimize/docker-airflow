"""Public art _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

#from dags.public_art.public_art_jobs import *
from dags.public_art.public_art_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['public_art']

#: Dag spec
dag = DAG(dag_id='public_art', default_args=args, schedule_interval=schedule)

public_art_latest_only = LatestOnlyOperator(task_id='public_art_latest_only', dag=dag)

#: Get public art from NetX, process, output prod file
get_public_art = PythonOperator(
    task_id='get_public_art',
    python_callable=get_public_art,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod art file to S3
upload_public_art = S3FileTransferOperator(
    task_id='upload_public_art',
    source_base_path=conf['prod_data_dir'],
    source_key='public_art_locations_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='public_art/public_art_locations_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_public_art_md = get_seaboard_update_dag('public-art.md', dag)


#: Execution rules
#: public_art_latest_only must run before get_public_art
get_public_art.set_upstream(public_art_latest_only)
#: get_public_art must run before file upload
upload_public_art.set_upstream(get_public_art)
#: upload_gid_requests must succeed before updating github
update_public_art_md.set_upstream(upload_public_art)
