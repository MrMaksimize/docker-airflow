"""Campaign finance _dags file."""
import re
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_email_operator import PoseidonEmailWithPythonOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email


from trident.util.seaboard_updates import *

from dags.netfile.netfile2_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['campaign_fin']
start_date = general.start_date['campaign_fin']
cur_yr = general.get_year()

#: Dag spec
dag = DAG(dag_id='campaign_fin_reports',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

#: Get 460A transactions
schedule_460A = PythonOperator(
    task_id='get_transactions_a',
    python_callable=get_transactions_a,
    
    dag=dag)

#: Get 460B1 transactions
schedule_460B1 = PythonOperator(
    task_id='get_transactions_b',
    python_callable=get_transactions_b,
    
    dag=dag)

#: Get 460C transactions
schedule_460C = PythonOperator(
    task_id='get_transactions_c',
    python_callable=get_transactions_c,
    
    dag=dag)

#: Get 460D transactions
schedule_460D = PythonOperator(
    task_id='get_transactions_d',
    python_callable=get_transactions_d,
    
    dag=dag)

#: Get summary transactions
schedule_sum = PythonOperator(
    task_id='get_transactions_summary',
    python_callable=get_transactions_summary,
    
    dag=dag)

#: Get 497 transactions
schedule_497 = PythonOperator(
    task_id='get_transactions_497',
    python_callable=get_transactions_497,
    
    dag=dag)

#: Get 496 transactions
schedule_496 = PythonOperator(
    task_id='get_transactions_496',
    python_callable=get_transactions_496,
    
    dag=dag)

#: Combine all transactions
combine_schedules = PythonOperator(
    task_id='combine_all_schedules',
    python_callable=combine_all_schedules,
    
    dag=dag)

#: Upload prod transactions file to S3
upload_fin_support = S3FileTransferOperator(
    task_id='upload_financial_support',
    source_base_path=conf['prod_data_dir'],
    source_key='financial_support_'+str(cur_yr)+'_datasd_v1.csv',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}", # What is this supposed to be?
    dest_s3_key='campaign_fin/financial_support_'+str(cur_yr)+'_datasd_v1.csv',
    
    replace=True,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'financial_trans_election_comms'},
    
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
    
    dag=dag)

#: Execution rules

schedule_460A >> schedule_460B1 >> schedule_460C >> schedule_460D >> schedule_sum
schedule_sum >> schedule_497 >> schedule_496 >> combine_schedules >> upload_fin_support
upload_fin_support >> [update_fin_support_md,update_json_date] >> send_committee_report
