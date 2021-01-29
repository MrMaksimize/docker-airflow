"""PD Hate Crimes _dags file."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from dags.pd.pd_hate_crimes_jobs import *
from trident.util import general
from trident.util.notifications import afsys_send_email

from trident.util.seaboard_updates import *

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['pd_hc']

dag = DAG(
    dag_id='pd_hate_crimes',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['pd_hc'],
    catchup=False
    )


#: Get collisions data from FTP and save to temp folder
get_hc_data = BashOperator(
    task_id='get_data',
    bash_command=get_data(),
    dag=dag)

#: Process collisions data and save result to prod folder
process_hc_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag)

#: Upload prod file to S3
hc_to_S3 = S3FileTransferOperator(
    task_id='prod_file_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='hate_crimes_datasd.csv',
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_key='pd/hate_crimes_datasd.csv',
    dag=dag)

#: Update data inventory json
update_hc_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'hate_crimes'},
    dag=dag)

#: Update portal modified date
update_pd_hc_md = get_seaboard_update_dag('police-hate-crimes.md', dag)

#: Execution rules:

get_hc_data >> process_hc_data >> hc_to_S3 >> [update_hc_date, update_pd_hc_md]
