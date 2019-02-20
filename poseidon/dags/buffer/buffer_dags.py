"""inventory_dags file."""
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from poseidon.util import general
from poseidon.util.notifications import notify
from poseidon.dags.buffer.buffer_jobs import *


conf = general.config

args = general.args

schedule = general.schedule['buffer_post_promo']

dag = DAG(dag_id='buffer_post_promo', default_args=args, schedule_interval=schedule)


buffer_post_latest_only = LatestOnlyOperator(task_id='buffer_post_latest_only', dag=dag)


#: Inventory Doc To CSV
buffer_add_tweet_mrm = PythonOperator(
    task_id='buffer_add_tweet_mrm',
    python_callable=queue_random_buffer_message,
    provide_context=True,
    op_kwargs={
        'gdoc_url':
        'https://docs.google.com/spreadsheets/d/1T7IIRH_8u5cRluQHOyrR7cW60IKIVDgn3XFOO_2Qt2o/pub?gid=0&single=true&output=csv',
        'col_name': 'Message',
        'access_token': conf['mrm_buffer_access_token'],
        'profile_service': 'twitter'
    },
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)



#: Execution Rules
#: Latest only for buffer adds
buffer_add_tweet_mrm.set_upstream(buffer_post_latest_only)
