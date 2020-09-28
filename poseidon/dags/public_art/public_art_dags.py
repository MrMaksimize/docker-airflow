"""Public art _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG

from trident.util import general
from trident.util.geospatial import *

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

#: Get public art from NetX
get_public_art = PythonOperator(
    task_id='get_public_art',
    python_callable=get_public_art,
    dag=dag)

#: Process API output into prod file
process_public_art = PythonOperator(
    task_id='process_public_art',
    python_callable=process_public_art,
    dag=dag)

#: Create a shapefile and prep KML
update_geospatial = PythonOperator(
    task_id='public_art_gis',
    python_callable=update_geospatial,
    dag=dag)

#: Upload prod art file to S3
upload_public_art = S3FileTransferOperator(
    task_id='upload_public_art',
    source_base_path=conf['prod_data_dir'],
    source_key='public_art_locations_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
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
get_public_art >> process_public_art >> update_geospatial
process_public_art >> upload_public_art >> [update_public_art_md,update_json_date]
