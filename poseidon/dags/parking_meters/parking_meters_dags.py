"""Parking meters _dags file."""
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email

from dags.parking_meters.parking_meters_jobs import *
from dags.parking_meters.parking_meters_subdags import *
from trident.util.seaboard_updates import *
from datetime import datetime, timedelta

args = general.args
schedule = general.schedule['parking_meters']
start_date = general.start_date['parking_meters']
conf = general.config

dag = DAG(
    dag_id='parking_meters',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False)

# Daily files sometimes contain transactions multiple year
# But files are created per calendar year

run_year = datetime.utcnow().year

#: Downloads all parking files from FTP
get_parking_files = PythonOperator(
    task_id='get_parking_files',
    python_callable=download_latest,
    provide_context=True,
    dag=dag)

#: Joins downloaded files from ftp to production
build_prod_file = PythonOperator(
    task_id='build_prod_file',
    python_callable=build_prod_file,
    provide_context=True,
    op_kwargs={'year': run_year},
    dag=dag)

#: Create aggregation files
build_curr_agg = SubDagOperator(
    task_id='create_curr_agg',
    subdag=create_current_subdag(run_year),
    dag=dag)

#: Create aggregation files
build_prev_agg = SubDagOperator(
    task_id='create_prev_agg',
    subdag=create_prev_subdag(run_year-1),
    dag=dag)

#: Upload files
upload_curr_s3 = SubDagOperator(
    task_id='upload_curr_files',
    subdag=upload_curr_files(run_year),
    dag=dag)

#: Upload files
upload_prev_s3 = SubDagOperator(
    task_id='upload_prev_files',
    subdag=upload_prev_files(run_year-1),
    dag=dag)

agg_branch = BranchPythonOperator(
    task_id='check_for_agg',
    provide_context=True,
    python_callable=check_agg,
    dag=dag)

year_branch = BranchPythonOperator(
    task_id='check_for_last_year',
    provide_context=True,
    trigger_rule='none_failed',
    python_callable=check_year,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'parking_meters_transactions'},    
    dag=dag)



#: Update portal modified date
update_parking_trans_md = get_seaboard_update_dag('parking-meters-transactions.md', dag)

get_parking_files >> build_prod_file >> agg_branch >> year_branch >> update_json_date >> update_parking_trans_md
agg_branch >> build_curr_agg >> upload_curr_s3 >> year_branch
year_branch >> build_prev_agg >> upload_prev_s3
upload_prev_s3 >> update_json_date

