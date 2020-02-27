"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify
from trident.util.seaboard_updates import *

from dags.streets.streets_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['streets']
start_date = general.start_date['streets']

#: Dag spec
dag = DAG(dag_id='streets', 
    default_args=args, 
    start_date=start_date, 
    schedule_interval=schedule,
    catchup=False
    )

#: Get streets data from DB
get_streets_data = PythonOperator(
    task_id='get_streets_paving_data',
    python_callable=get_streets_paving_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get streets data from DB
base_data = PythonOperator(
    task_id='process_base_data',
    python_callable=create_base_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process data for public
portal_data = PythonOperator(
    task_id='process_sdif',
    python_callable=create_mode_data,
    op_kwargs={'mode': 'sdif'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process data for imcat
imcat_data = PythonOperator(
    task_id='process_imcat',
    python_callable=create_mode_data,
    op_kwargs={'mode': 'imcat'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload imcat streets file to S3
upload_imcat_data = S3FileTransferOperator(
    task_id='upload_streets_data_imcat',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_paving_imcat_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sd_paving_imcat_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload sdif streets file to S3
upload_sdif_data = S3FileTransferOperator(
    task_id='upload_streets_data_sdif',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_paving_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sd_paving_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'streets_repair_projects'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

create_esri_file = PythonOperator(
    task_id='create_streets_gis',
    python_callable=create_arcgis_base,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

send_esri_layers = PythonOperator(
    task_id='send_esri_layers',
    python_callable=send_esri_layers,
    provide_context=True,
    op_kwargs={'mode': 'streets_repair_projects'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: send file update email to interested parties
#send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    #task_id='send_last_file_updated',
    #to='chudson@sandiego.gov',
    #subject='IMCAT Streets File Updated',
    #file_url='http://{}/{}'.format(conf['dest_s3_bucket'],
                                   #'tsw/sd_paving_imcat_datasd_v1.csv'),
    #on_failure_callback=notify,
    #on_retry_callback=notify,
    #on_success_callback=notify,
    #dag=dag)

#: Update portal modified date
update_streets_md = get_seaboard_update_dag('streets-repair-projects.md', dag)

#: Execution order

get_streets_data >> base_data >> [portal_data,imcat_data] 
portal_data >> [upload_sdif_data, create_esri_file]
imcat_data >> upload_imcat_data
upload_sdif_data >> [update_json_date,update_streets_md]
create_esri_file >> send_esri_file

#: email notification is sent after the data was uploaded to S3
#send_last_file_updated_email.set_upstream(upload_imcat_data)