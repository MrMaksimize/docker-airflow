""" PD docs _dags file. """

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.pd_docs.pd_docs_jobs import *

# Required variables

from airflow.models import Variable
args = general.args
conf = general.config
schedule = general.schedule['pd_docs']
start_date = general.start_date['pd_docs']


#: Required DAG definition
dag = DAG(dag_id='pd_docs',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

# Optional tasks. Use what you need.

#: Basic Python operator
create_data = PythonOperator(
    task_id='get_themis_contents',
    python_callable=get_themis_contents,
    dag=dag)

#: Upload to S3
upload_data = S3FileTransferOperator(
    task_id='template_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='/',
    replace=True,
    dag=dag)


#: Required execution rules
create_data >> upload_data
