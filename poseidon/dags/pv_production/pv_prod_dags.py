from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import notify

from dags.pv_prod.pv_prod_jobs import *

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = general.schedule['pv_prod']
start_date = general.start_date['pv_prod']
conf = general.config
cur_yr = general.get_year()