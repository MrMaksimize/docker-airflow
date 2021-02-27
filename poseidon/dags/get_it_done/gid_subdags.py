"""Get It Done _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.get_it_done.gid_jobs import *
from trident.util.seaboard_updates import get_seaboard_update_dag

from datetime import date

args = general.args
conf = general.config
schedule = general.schedule['get_it_done']
start_date = general.start_date['get_it_done']

def spatial_join_subdag():
  """
  Generate a DAG that performs
  spatial joins 
  """

  dag_subdag = DAG(
    dag_id='get_it_done.spatial_joins',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
  )

  join_council_districts = PythonOperator(
    task_id='join_council_districts',
    python_callable=join_requests_polygons,
    op_kwargs={'tempfile':'gid_ref',
    'geofile':'council_districts',
    'drop_cols':['objectid',
        'area',
        'perimeter',
        'name',
        'phone',
        'website'],
    'outfile':'gid_cd'},
    dag=dag_subdag)

  join_community_plan = PythonOperator(
    task_id='join_community_plan',
    python_callable=join_requests_polygons,
    op_kwargs={'tempfile':'gid_cd',
    'geofile':'cmty_plan',
    'drop_cols':['objectid',
          'acreage'],
    'outfile':'gid_cp'},
    dag=dag_subdag)

  join_parks = PythonOperator(
    task_id='join_parks',
    python_callable=join_requests_polygons,
    op_kwargs={'tempfile':'gid_cp',
    'geofile':'parks',
    'drop_cols':['objectid',
          'gis_acres',
          'location'],
    'outfile':'gid_parks'},
    dag=dag_subdag)

  join_council_districts >> join_community_plan >> join_parks

  return dag_subdag

def service_name_subdag():

    """
    Create a subdag that produces
    files per service
    """

    dag_subdag = DAG(
        dag_id='get_it_done.service_names',
        default_args=args,
        start_date=start_date,
        schedule_interval=schedule,
        catchup=False
      )

    services = [
    'graffiti_removal',
    'illegal_dumping',
    'pothole',
    '72_hour_violation']

    for service in services:
        service_name = service.replace("_"," ").title()
        machine_service_name = service
        md_name = service.replace("_","-")

        get_task = PythonOperator(
            task_id=f"get_{machine_service_name}",
            python_callable=get_requests_service_name,
            op_kwargs={
                'service_name': service_name,
                'machine_service_name': machine_service_name
            },
            dag=dag_subdag)

        upload_task = S3FileTransferOperator(
            task_id=f"upload_{machine_service_name}",
            source_base_path=conf['prod_data_dir'],
            source_key=f"get_it_done_{machine_service_name}_requests_datasd_v1.csv",
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f"get_it_done_311/{machine_service_name}_requests_datasd_v1.csv",
            replace=True,
            dag=dag_subdag)

        md_update = get_seaboard_update_dag(f'gid-{md_name}.md', dag_subdag)

        get_task >> upload_task >> md_update

    return dag_subdag

def upload_files_subdag():

    """
    Generate a DAG to upload all relevant files
    """
    dag_subdag = DAG(
        dag_id='get_it_done.upload_files',
        default_args=args,
        start_date=start_date,
        schedule_interval=schedule,
        catchup=False
      )

    curr_year = date.today().year

    for year in range(2016,curr_year+1):

        upload_task = S3FileTransferOperator(
            task_id=f'upload_{year}',
            source_base_path=conf['prod_data_dir'],
            source_key=f'get_it_done_{year}_requests_datasd_v1.csv',
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f'get_it_done_311/get_it_done_{year}_requests_datasd_v1.csv',
            replace=True,
            dag=dag_subdag)

    upload_all = S3FileTransferOperator(
        task_id=f'upload_full',
        source_base_path=conf['prod_data_dir'],
        source_key=f'get_it_done_requests_datasd.csv',
        dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
        dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
        dest_s3_key=f'get_it_done_311/get_it_done_requests_datasd.csv',
        replace=True,
        dag=dag_subdag)

    return dag_subdag


