"""Template _dags file."""

# Required imports

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.util.seaboard_updates import *
from trident.util import general
from trident.util.notifications import afsys_send_email


#### You must update these with the paths to the corresponding files ####
from dags.templates.template_jobs import *
from dags.templates.template_subdags import *

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

args = general.args
conf = general.config
schedule = general.schedule['']
start_date = general.start_date['']

# Optional variable
email_recips = conf['mail_notify']

#: Required DAG definition
dag = DAG(dag_id='template',
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
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Python operator with context
template_task_context = PythonOperator(
    task_id='python_task_context',
    provide_context=True,
    python_callable=python_context,
    on_failure_callback=afsys_send_email,
    dag=dag)

# The following two tasks call the same jobs function
# But pass in different arguments

#: Python operator with kwargs
template_task_kwargs1 = PythonOperator(
    task_id='python_task_kwarg1',
    op_kwargs={'mode': 'mode1'},
    python_callable=python_kwarg,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Python operator with kwargs
template_task_kwargs2 = PythonOperator(
    task_id='python_task_kwarg2',
    op_kwargs={'mode': 'mode2'},
    python_callable=python_kwarg,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Python operator with context and kwargs
template_task_both = PythonOperator(
    task_id='python_task_both',
    op_kwargs={'mode': 'mode1'},
    provide_context=True,
    python_callable=python_both,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Bash operator
template_bash_task = BashOperator(
    task_id='template_bash_task',
    bash_command=bash_task(),
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Sonar - calc a metric for a day, week, or month
template_sonar = PoseidonSonarCreator(
    task_id='template_sonar_task',
    range_id='', # must be one of: today, days_7, or days_30
    value_key='',
    value_desc='',
    python_callable=sonar_task,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: An email with a list of info
template_email_report = PoseidonEmailWithPythonOperator(
    task_id='template_email',
    to='',
    subject='',
    template_id='', # from Send With Us
    dispatch_type='sonar_dispatch',
    python_callable=email_task,
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Send an email a file was updated
template_email_updated = PoseidonEmailFileUpdatedOperator(
    task_id='template_send_updated',
    to=email_recips,
    subject='File Updated',
    file_url="http://seshat.datasd.org/dsd/dsd_permits_all_pts.csv",
    message='<p>DSD permits have been updated.</p>' \
            + '<p>Please follow the link below to download.</p>',
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Shortcircuit operator
template_short_circuit = ShortCircuitOperator(
    task_id='template_short_circuit',
    provide_context=True,
    python_callable=short_circuit_task,
    dag=dag)

#: Branch operator
template_branch = BranchPythonOperator(
    task_id='check_for_template',
    provide_context=True,
    python_callable=template_check,
    dag=dag)

#: Subdag operator
template_subdag = SubDagOperator(
    task_id='template_subdag',
    subdag=create_subdag_operators(),
    dag=dag,
  )

#: Upload to S3
upload_data = S3FileTransferOperator(
    task_id='template_upload',
    source_base_path=conf['prod_data_dir'],
    source_key='',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='/',
    on_failure_callback=afsys_send_email,
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
    on_failure_callback=afsys_send_email,
    dag=dag)

#: Required execution rules
template_task_basic >> template_task_context >> template_branch
template_branch >> [template_email_updated, template_task_both] 
template_task_both >> template_short_circuit >> [template_task_kwargs1,template_task_kwargs2]
template_task_kwargs2 >> template_subdag >> [template_sonar,template_email_report]
template_task_kwargs1 >> [update_md,update_json_date]
