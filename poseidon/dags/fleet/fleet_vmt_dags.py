"""Fleet VMT _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.fleet.fleet_vmt_jobs import *

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
dag = DAG(dag_id='fleet_vmt',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Basic Python operator
get_calamp = PythonOperator(
    task_id='get_calamp',
    provide_context=True,
    python_callable=download_calamp_daily,
    dag=dag)

#: Basic Python operator
process_calamp = PythonOperator(
    task_id='process_calamp',
    provide_context=True,
    python_callable=process_calamp_daily,
    dag=dag)

get_calamp >> process_calamp
