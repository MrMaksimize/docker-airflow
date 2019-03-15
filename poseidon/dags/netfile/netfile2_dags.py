"""Campaign finance _dags file."""
import re
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_email_operator import PoseidonEmailWithPythonOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from trident.util.seaboard_updates import *

from dags.netfile.netfile2_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['campaign_fin']
start_date = general.start_date['campaign_fin']
cur_yr = general.get_year()

#: Dag spec
dag = DAG(dag_id='campaign_fin_reports', default_args=args, start_date=start_date, schedule_interval=schedule)

campaign_fin_latest_only = LatestOnlyOperator(task_id='campaign_fin_latest_only', dag=dag)

#: Get 460A transactions
schedule_460A = PythonOperator(
    task_id='get_transactions_a',
    python_callable=get_transactions_a,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get 460B1 transactions
schedule_460B1 = PythonOperator(
    task_id='get_transactions_b',
    python_callable=get_transactions_b,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get 460C transactions
schedule_460C = PythonOperator(
    task_id='get_transactions_c',
    python_callable=get_transactions_c,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get 460D transactions
schedule_460D = PythonOperator(
    task_id='get_transactions_d',
    python_callable=get_transactions_d,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get summary transactions
schedule_sum = PythonOperator(
    task_id='get_transactions_summary',
    python_callable=get_transactions_summary,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get 497 transactions
schedule_497 = PythonOperator(
    task_id='get_transactions_497',
    python_callable=get_transactions_497,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get 496 transactions
schedule_496 = PythonOperator(
    task_id='get_transactions_496',
    python_callable=get_transactions_496,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Combine all transactions
combine_schedules = PythonOperator(
    task_id='combine_all_schedules',
    python_callable=combine_all_schedules,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod transactions file to S3
upload_fin_support = S3FileTransferOperator(
    task_id='upload_financial_support',
    source_base_path=conf['prod_data_dir'],
    source_key='financial_support_'+str(cur_yr)+'_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'], # What is this supposed to be?
    dest_s3_key='campaign_fin/financial_support_'+str(cur_yr)+'_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_fin_support_md = get_seaboard_update_dag('financial-support-candidates-and-ballot-measures-election.md', dag)

#: Email new committees
send_committee_report = PoseidonEmailWithPythonOperator(
    task_id='send_committee_report',
    to='abower@sandiego.gov',
    subject='Campaign committees update',
    template_id='tem_7xCrDCTyvjMGS9VpBM8rRmwD',
    dispatch_type='sonar_dispatch',
    python_callable=send_comm_report,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execution rules
#: campaign_fin_latest_only must run before schedule_460A
schedule_460A.set_upstream(campaign_fin_latest_only)
#: schedule_460A must run before schedule_460B1
schedule_460B1.set_upstream(schedule_460A)
#: schedule_460B1 must run before schedule_460C
schedule_460C.set_upstream(schedule_460B1)
#: schedule_460C must run before schedule_460D
schedule_460D.set_upstream(schedule_460C)
#: schedule_460D must run before schedule_sum
schedule_sum.set_upstream(schedule_460D)
#: schedule_sum must run before schedule_497
schedule_497.set_upstream(schedule_sum)
#: schedule_497 must run before schedule_496
schedule_496.set_upstream(schedule_497)
#: schedule_496 must run before combine_schedules
combine_schedules.set_upstream(schedule_496)
#: combine_schedules must run before file upload
upload_fin_support.set_upstream(combine_schedules)
#: file upload must run before github updated
update_fin_support_md.set_upstream(upload_fin_support)
#: update file must run before starting job 2
send_committee_report.set_upstream(update_fin_support_md)
