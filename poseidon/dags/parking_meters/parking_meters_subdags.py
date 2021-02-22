"""Parking meters subdags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import afsys_send_email
from dags.parking_meters.parking_meters_jobs import *

from trident.util.seaboard_updates import *
conf = general.config
args = general.args
schedule = general.schedule['parking_meters']
start_date = general.start_date['parking_meters']

def create_current_subdag(year):
	"""
	Generate a DAG to be used as a subdag 
	that creates agg files
	"""
	
	dag_subdag = DAG(
	dag_id='parking_meters.create_curr_agg',
	default_args=args,
	start_date=start_date,
	schedule_interval=schedule,
	catchup=False
	)

	agg_type = ['pole_by_month','pole_by_mo_day']

	for agg in agg_type:

	    #: Builds by day aggregation
		build_by_day_aggregation = PythonOperator(
		    task_id=f"create_{agg}_{year}",
		    python_callable=build_aggregation,
		    op_kwargs={'agg_type': agg,'agg_year':year},
		    provide_context=True,
		    
		    dag=dag_subdag)

	return dag_subdag


def create_prev_subdag(year):
	"""
	Generate a DAG to be used as a subdag 
	that creates agg files 
	"""

	dag_subdag = DAG(
	dag_id='parking_meters.create_prev_agg',
	default_args=args,
	start_date=start_date,
	schedule_interval=schedule,
	catchup=False
	)

	agg_type = ['pole_by_month','pole_by_mo_day']

	for agg in agg_type:

	    #: Builds by day aggregation
		build_by_day_aggregation = PythonOperator(
		    task_id=f"create_{agg}_{year}",
		    python_callable=build_aggregation,
		    op_kwargs={'agg_type': agg,'agg_year':year},
		    provide_context=True,
		    
		    dag=dag_subdag)

	return dag_subdag

def upload_curr_files(year):
	"""
	Generate a DAG to be used as a subdag 
	to upload files for this year
	"""

	dag_subdag = DAG(
	dag_id='parking_meters.upload_curr_files',
	default_args=args,
	start_date=start_date,
	schedule_interval=schedule,
	catchup=False
	)

	file_list = {'full': f'treas_parking_payments_{year}_datasd_v2.csv',
	'by_month': f'treas_meters_{year}_pole_by_month_datasd_v2.csv',
	'by_day': f'treas_meters_{year}_pole_by_mo_day_datasd_v2.csv'}

	names = [*file_list]

	for file in names:

		#: Uploads the generated agg file
		upload_by_month_agg = S3FileTransferOperator(
		task_id=f'upload_{file}_{year}',
		source_base_path=conf['prod_data_dir'],
		source_key=file_list.get(file),
		dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
		dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
		dest_s3_key=f'parking_meters/{file_list.get(file)}',
		replace=True,
		
		dag=dag_subdag)

	return dag_subdag

def upload_prev_files(year):
	"""
	Generate a DAG to be used as a subdag 
	to upload files for this year
	"""

	dag_subdag = DAG(
	dag_id='parking_meters.upload_prev_files',
	default_args=args,
	start_date=start_date,
	schedule_interval=schedule,
	catchup=False
	)

	file_list = {'full': f'treas_parking_payments_{year}_datasd_v2.csv',
	'by_month': f'treas_meters_{year}_pole_by_month_datasd_v2.csv',
	'by_day': f'treas_meters_{year}_pole_by_mo_day_datasd_v2.csv'}

	names = [*file_list]

	for file in names:

		#: Uploads the generated agg file
		upload_by_month_agg = S3FileTransferOperator(
		task_id=f'upload_{file}_{year}',
		source_base_path=conf['prod_data_dir'],
		source_key=file_list.get(file),
		dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
		dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
		dest_s3_key=f'parking_meters/{file_list.get(file)}',
		replace=True,
		
		dag=dag_subdag)

	return dag_subdag