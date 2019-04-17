"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general
from trident.util.notifications import notify

# - You must replace this with the path to the corresponding jobs file
from dags.templates.template_jobs import *

# Optional imports

from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator

import re
import glob

# Required variables

args = general.args
conf = general.config
# You must replace this with actual schedule
schedule = general.schedule['get_it_done']
# You must replace this with actual start date
start_date = general.start_date['get_it_done']

#: Dag spec
# Replace this dag id with actual dag id
dag = DAG(dag_id='template_dag',
        default_args=args,
        schedule_interval=schedule,
        start_date=start_date)

# You must have a latest only task
template_latest_only = LatestOnlyOperator(task_id='template_latest_only', dag=dag)

#: Call a python function from the jobs file example
second_template_task = PythonOperator(
    task_id='template_python_task', # This is a task name you choose
    python_callable=template_python_task, # This must match the function from jobs
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execute a bash command function from the jobs file example
third_template_task = BashOperator(
    task_id='template_bash_task',
    bash_command=ftp_download_wget(), # Matches function from jobs file
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Send Sonar report example
template_sonar = PoseidonSonarCreator(
    task_id='template_sonar_task',
    range_id='days_30',
    value_key='gid_potholes_closed',
    value_desc='Potholes Closed',
    python_callable=build_gid_sonar_ph_closed,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Send an email with info example
template_email_report = PoseidonEmailWithPythonOperator(
    task_id='send_report',
    to='abower@sandiego.gov',
    subject='Your report',
    template_id='tem_7xCrDCTyvjMGS9VpBM8rRmwD',
    dispatch_type='sonar_dispatch',
    python_callable=send_comm_report, # Replace with jobs function
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)



# An example of using a loop to create tasks

loop_tasks = []

for i in a_list:
    
    template_loop_task = PythonOperator(
        task_id=f'get_{i}',
        python_callable=template_loop_task, # Again, this must match a function from jobs
        op_kwargs={},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    loop_tasks.append(template_loop_task)

    #: Explain what task is upstream
    template_loop_task.set_upstream(task_to_precede)
    #: Explain any tasks that need to occur downstream
    template_loop_task.set_downstream(task_to_follow)

# An example of upload and update tasks based on a loop

# This one uses a wildcard pattern to search prod
# Good for data separated by calendar year
file_name = f"{conf['prod_data_dir']}/file_name_*.csv"
files = [os.path.basename(x) for x in glob.glob(file_name)]

for index, file_ in enumerate(files):
    name = file_.split('.')[0]
    name_parts = name.split('_')
    # Isolate parts of file name for task name
    task_name = '_'.join(name_parts[3:-2])
    # Isolate parts of file name for md name
    md_name = '-'.join(name_parts[3:-2])

    #: Upload prod file to S3
    template_upload_task = S3FileTransferOperator(
        task_id='upload_' + task_name,
        source_base_path=conf['prod_data_dir'],
        source_key=f'file_name_{task_name}_datasd.csv',
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key=f'folder/file_name_{task_name}_datasd.csv',
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)
    
    #: Explain what task is upstream, probably the last task
    template_upload_task.set_upstream(third_template_task)

    
    template_md_update_task = get_seaboard_update_dag(md_name, dag)
        
    #: update md task must run after the upload task
    template_md_update_task.set_upstream(template_upload_task)

#: Execution rules
#: latest_only must run before anything else
second_template_task.set_upstream(template_latest_only)
#: second_template_task must run before third_template_task
third_template_task.set_upstream(second_template_task)

