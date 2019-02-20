"""Get It Done Potholes reporting _dags file."""
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from poseidon.util import general
from poseidon.util.notifications import notify

from poseidon.dags.get_it_done.gid_potholes_counts_jobs import get_sf_gid_potholes

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['gid_potholes']

#: Dag spec
dag = DAG(dag_id='gid_potholes', default_args=args, schedule_interval=schedule)

gid_potholes_latest_only = LatestOnlyOperator(task_id='gid_potholes_latest_only', dag=dag)

#: Get aggregated potholes counts per council district, process, prod
get_sf_gid_potholes = PythonOperator(
    task_id='get_sf_gid_potholes',
    python_callable=get_sf_gid_potholes,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod file to S3
upload_gid_potholes = S3FileTransferOperator(
    task_id='upload_gid_potholes',
    source_base_path=conf['prod_data_dir'],
    source_key='potholes_agg_districts_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='get_it_done_311/potholes_agg_districts_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)


send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    task_id='send_last_file_updated',
    to='ggerhant@sandiego.gov,ahempton@sandiego.gov,kreeser@sandiego.gov',
    subject='Pothole File Updated',
    file_url='http://{}/{}'.format(
        conf['dest_s3_bucket'],
        'get_it_done_311/potholes_agg_districts_datasd.csv'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)



#: Execution rules
#: gid_potholes_latest_only must run before get_sf_gid_potholes
get_sf_gid_potholes.set_upstream(gid_potholes_latest_only)
#: get_sf_gid_potholes must run before file upload
upload_gid_potholes.set_upstream(get_sf_gid_potholes)
#: send last file updated email once upload_gid_potholes completes
send_last_file_updated_email.set_upstream(upload_gid_potholes)
