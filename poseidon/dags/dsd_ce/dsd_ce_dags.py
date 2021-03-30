"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.dsd_ce.dsd_ce_jobs import *

# Optional operator imports

# Required variables

from airflow.models import Variable
args = general.args
conf = general.config
schedule = '@daily' # Replace
start_date = general.default_date # Replace


#: Required DAG definition
dag = DAG(dag_id='dsd_ce',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

# Optional tasks. Use what you need.

#: Python operator with context
query_accela = PythonOperator(
    task_id='query_accela_ce',
    provide_context=True,
    python_callable=query_data,
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

#: Update a dataset page
update_md = get_seaboard_update_dag('', dag)

#: Update data catalog json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': ''},
    dag=dag)

#: Required execution rules
