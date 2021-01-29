"""Streets _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from trident.util import general
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

from dags.google_analytics.google_analytics_jobs import *

args = general.args
conf = general.config
schedule = general.schedule['ga_portal']
start_date = general.start_date['ga_portal']

reports_kwargs = {
  'all_users_sessions':{
    'mets':['users','sessions','sessionDuration','hits'],
    'dims':['date','hour','userType']
  },
  'all_traffic_sources':{
    'mets':['sessions'],
    'dims':['date','source','referralPath','keyword']
  },
  'all_devices_platforms':{
    'mets':['sessions'],
    'dims':['date','deviceCategory','browser','browserVersion','operatingSystem']
  },
  'all_pages':{
    'mets':['entrances','exits','uniquePageviews','avgTimeOnPage','pageviews','users','bounceRate'],
    'dims':['date','hostname','pagePathLevel1','pagePathLevel2','pagePathLevel3']
  }
}

def create_subdag_operators():
  """
  Generate a DAG to be used as a subdag 
  that updates ESRI map layers
  """

  dag_subdag = DAG(
    dag_id='ga_sandiego.get_upload_reports',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  reports_list = [*reports_kwargs]
  
  for report in reports_list:

    request_params = reports_kwargs.get(report)

    get_task = PythonOperator(
        task_id=f'get_{report}',
        op_kwargs={'view_id': '83386397',
        'mets':request_params.get('mets'),
        'dims':request_params.get('dims'),
        'out_path':report,
        'range':'monthly' # can be monthly, weekly, daily
        },
        provide_context=True,
        python_callable=ga_batch_get,
        dag=dag_subdag,
      )

    process_task = PythonOperator(
        task_id=f'process_{report}',
        op_kwargs={'out_path':report,
        'dims':request_params.get('dims')
        },
        python_callable=process_batch_get,
        dag=dag_subdag,
      )

    upload_task = S3FileTransferOperator(
            task_id=f"upload_{report}",
            source_base_path=conf['prod_data_dir'],
            source_key=f"{report}_datasd.csv",
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f"web_analytics/{report}_datasd.csv",
            replace=True,
            dag=dag_subdag)

    get_task >> process_task >> upload_task

  return dag_subdag