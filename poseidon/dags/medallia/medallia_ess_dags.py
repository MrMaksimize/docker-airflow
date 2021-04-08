"""Medallia ESS _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.medallia.medallia_jobs import *
#from dags.templates.template_subdags import *

# Optional operator imports
from airflow.operators.python_operator import BranchPythonOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import ShortCircuitOperator
from trident.operators.r_operator import RScriptOperator
from trident.operators.r_operator import RShinyDeployOperator
from trident.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator
from trident.operators.poseidon_email_operator import PoseidonEmailWithPythonOperator

# Required variables

from airflow.models import Variable
args = general.args
conf = general.config
schedule = '@daily' # Replace
start_date = general.default_date # Replace


#: Required DAG definition
dag = DAG(dag_id='medallia_ess',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

# Optional tasks. Use what you need.

#: Basic Python operator
template_task_basic = PythonOperator(
    task_id='python_task_basic',
    python_callable=python_basic,
    dag=dag)

#: Python operator with context
template_task_context = PythonOperator(
    task_id='python_task_context',
    provide_context=True,
    python_callable=python_context,
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

