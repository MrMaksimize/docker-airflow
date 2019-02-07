from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from poseidon.util import general
from poseidon.util.notifications import notify

from poseidon.dags.get_it_done.gid_ava_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['gid_ava']

#: Dag spec
dag = DAG(dag_id='gid_ava', default_args=args, schedule_interval=schedule)

gid_ava_latest_only = LatestOnlyOperator(task_id='gid_ava_latest_only', dag=dag)

#: Get ava data
get_sf_gid_ava = PythonOperator(
    task_id='get_sf_gid_ava',
    python_callable=get_sf_gid_ava,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get police beats data
get_police_beats = BashOperator(
    task_id='get_police_beats',
    bash_command=wget_police_beats(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Spatially join beats to ava reports
join_police_beats = PythonOperator(
    task_id='join_police_beats',
    python_callable=join_police_beats,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Upload AVA prod file to S3
upload_gid_ava = S3FileTransferOperator(
    task_id='upload_gid_ava',
    source_base_path=conf['prod_data_dir'],
    source_key='gid_ava_yesterday_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='get_it_done_311/gid_ava_yesterday_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)


send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    task_id='send_last_file_updated',
    to='ggerhant@sandiego.gov,ahempton@sandiego.gov',
    subject='Abandoned Vehicles File Updated',
    file_url='http://{}/{}'.format(
        conf['dest_s3_bucket'],
        'get_it_done_311/gid_ava_yesterday_datasd.csv'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Execution rules
#: gid_ava_latest_only must run before get_sf_gid_ava
get_sf_gid_ava.set_upstream(gid_ava_latest_only)
#: get_sf_gid_ava must run before get_police_beats
get_police_beats.set_upstream(get_sf_gid_ava)
#: get_police_beats must run before join_police_beats
join_police_beats.set_upstream(get_police_beats)
#: join_police_beats must run before upload_gid_ava
upload_gid_ava.set_upstream(join_police_beats)
#: send last file updated email once upload_gid_ava completes
send_last_file_updated_email.set_upstream(upload_gid_ava)
