from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from dags.cityiq.cityiqjobs import get_token_response, get_assets, get_asset_details
from trident.util import general
from trident.util.notifications import notify

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
start_date = general.start_date['cityiq'] 
#: Dag spec
dag = DAG(dag_id='cityiq', default_args=args, start_date=start_date, schedule_interval=general.schedule['cityiq'])

get_token_response = PythonOperator(
    task_id='get_token_response',
    python_callable=get_token_response,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_assets = PythonOperator(
    task_id='get_assets',
    python_callable=get_assets,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_asset_details = PythonOperator(
    task_id='get_asset_details',
    python_callable=get_asset_details,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

'''
testupload = S3FileTransferOperator(
        task_id='upload_test',
        source_base_path=conf['prod_data_dir'],
        source_key='get_it_done_{}_requests_datasd.csv'.format(
            task_name),
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='get_it_done_311/get_it_done_{}_requests_datasd.csv'.
        format(task_name),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)
    )
'''