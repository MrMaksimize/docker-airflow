""" Google Analytics _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general

#### You must update these with the paths to the corresponding files ####
from dags.google_analytics.ga_portal_jobs import *
#from dags.google_analytics._subdags import *

# Optional operator imports
from airflow.operators.python_operator import BranchPythonOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import ShortCircuitOperator

# Required variables

args = general.args
conf = general.config
schedule = general.schedule['ga_portal']
start_date = general.start_date['ga_portal']

#: Required DAG definition
dag = DAG(dag_id='ga_portal',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Authenticate GA
create_keyfile = PythonOperator(
    task_id='create_client_secrets',
    python_callable=create_client_secrets,
    dag=dag)

#: Get users sessions report
get_users_sessions = PythonOperator(
    task_id='get_users_sessions',
    op_kwargs={'view_id': '124490020',
    'mets':['users','sessions','sessionDuration','hits'],
    'dims':['date','hour','userType'],
    'out_path':'portal_users_sessions'},
    provide_context=True,
    python_callable=ga_batch_get,
    dag=dag)

#: Get traffic report
get_traffic = PythonOperator(
    task_id='get_traffic',
    op_kwargs={'view_id': '124490020',
    'mets':['users','sessions','sessionDuration','hits'],
    'dims':['date','hour','userType'],
    'out_path':'portal_traffic_sources'},
    provide_context=True,
    python_callable=ga_batch_get,
    dag=dag)

#: Get devices report
get_devices = PythonOperator(
    task_id='get_devices',
    op_kwargs={'view_id': '124490020',
    'mets':['users','sessions','sessionDuration','hits'],
    'dims':['date','hour','userType'],
    'out_path':'portal_devices_platforms'},
    provide_context=True,
    python_callable=ga_batch_get,
    dag=dag)

#: Get pages report
get_pages = PythonOperator(
    task_id='get_pages',
    op_kwargs={'view_id': '124490020',
    'mets':['entrances','exits','uniquePageviews','avgTimeOnPage','pageviews','users'],
    'dims':['date','hour','userType'],
    'out_path':'portal_pages'},
    provide_context=True,
    python_callable=ga_batch_get,
    dag=dag)

#: Get events report
get_events = PythonOperator(
    task_id='get_events',
    op_kwargs={'view_id': '124490020',
    'mets':['users','sessions','sessionDuration','hits'],
    'dims':['date','hour','userType'],
    'out_path':'portal_events'},
    provide_context=True,
    python_callable=ga_batch_get,
    dag=dag)

create_keyfile >> get_users_sessions