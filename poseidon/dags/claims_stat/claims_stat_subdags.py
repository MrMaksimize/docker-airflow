"""This module contains dags and tasks for extracting data out of Claims Stat."""
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

from dags.claims_stat.claims_stat_jobs import *

from datetime import datetime, timedelta
from trident.util import general
from trident.util.notifications import afsys_send_email
from trident.operators.r_operator import RScriptOperator
from trident.operators.r_operator import RShinyDeployOperator
from trident.operators.poseidon_email_operator import PoseidonEmailFileUpdatedOperator

args = general.args
conf = general.config
schedule = general.schedule['claims_stat']
start_date = general.start_date['claims_stat']

claims_kwargs = {'tsw':['2116'],
'pd':['1914'],
'pud':['2000']}

def create_subdag():
    
    dag_subdag = DAG(
            dag_id='claims_stat.upload_deploy_email',
            default_args=args,
            start_date=start_date,
            schedule_interval=schedule,
            catchup=False)

    for org_name in [*claims_kwargs]:
        # dataset is tsw, pd, or pud
        # temp_kwargs = ORG_CODE_SUBSTR
        claim_orgs = claims_kwargs[org_name]
        email_org = org_name.upper()

        claims_filter = PythonOperator(
            task_id=f'claims_by_department_{org_name}',
            python_callable=claims_by_department,
            op_kwargs={'org_name':org_name,'claim_orgs':claim_orgs},
            dag=dag_subdag)

        #: Upload prod claims file to S3
        upload_claimstat_clean = S3FileTransferOperator(
            task_id=f'upload_claimstat_clean_{org_name}',
            source_base_path=conf['prod_data_dir'],
            source_key=f'claims_clean_datasd_{org_name}.csv',
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f'risk/claims_clean_datasd_{org_name}.csv',
            replace=True,
            dag=dag_subdag)

        deploy = BashOperator(
            task_id=f'deploy_dashboard_{org_name}',
            bash_command=deploy_dashboard(org_name),
            dag=dag_subdag)

        send_last_file_updated_email = PoseidonEmailFileUpdatedOperator(
            task_id=f'send_dashboard_updated_{org_name}',
            to="{{ var.value.MAIL_NOTIFY_CLAIMS_TSW }}",
            subject='Dashboard Updated',
            file_bucket=None,
            file_url=f"https://sandiego-panda.shinyapps.io/claims_{org_name}_{conf['env'].lower()}/",
            message='<p>The ClaimStat tool has been updated.</p>' \
                    + '<p>Please follow the link below to view the tool.</p>',
            dag=dag_subdag)

        claims_filter >> upload_claimstat_clean >> deploy >> send_last_file_updated_email

    return dag_subdag