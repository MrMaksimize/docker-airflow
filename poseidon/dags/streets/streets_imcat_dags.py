"""Streets imcat _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from poseidon.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator
from airflow.models import DAG

from poseidon.util import general
from poseidon.util.notifications import notify

from poseidon.dags.streets.streets_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['streets_sdif']

#: Dag spec
dag = DAG(dag_id='streets_imcat', default_args=args, schedule_interval=schedule)


#: Latest Only Operator for imcat
streets_latest_only = LatestOnlyOperator(task_id='imcat_latest_only', dag=dag)


#: Get streets data from DB
get_streets_data = PythonOperator(
    task_id='get_streets_data_imcat',
    python_callable=get_streets_paving_data,
    op_kwargs={'mode': 'imcat'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod streets file to S3
upload_streets_data = S3FileTransferOperator(
    task_id='upload_streets_data_imcat',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_paving_imcat_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sd_paving_imcat_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: send file update email to interested parties
send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
    task_id='send_last_file_updated',
    to='chudson@sandiego.gov,jlahmann@sandiego.gov,agomez@sandiego.gov',
    subject='IMCAT Streets File Updated',
    file_url='http://{}/{}'.format(conf['dest_s3_bucket'],
                                   'tsw/sd_paving_imcat_datasd.csv'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execution order

#: streets_latest_only must run before get_streets_data
get_streets_data.set_upstream(streets_latest_only)

#: upload_streets_data is dependent on successful run of get_streets_data
upload_streets_data.set_upstream(get_streets_data)

#: email notification is sent after the data was uploaded to S3
send_last_file_updated_email.set_upstream(upload_streets_data)
