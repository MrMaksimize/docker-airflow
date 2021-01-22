"""Get It Done _dags file."""
import re
import glob
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

from trident.util import general
from trident.util.notifications import afsys_send_email
from trident.util.seaboard_updates import *

from dags.gid_case_history.case_history_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['gid_case_history']
start_date = general.start_date['gid_case_history']

#: Dag spec
dag = DAG(dag_id='gid_case_history',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date,
        catchup=False
        )

#: Get GID CaseHistory from Salesforce
get_streets_requests = PythonOperator(
    task_id='get_case_history',
    python_callable=get_gid_streets,
    dag=dag)
          
#: Execution rules
