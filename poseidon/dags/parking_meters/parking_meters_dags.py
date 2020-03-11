"""Parking meters _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from trident.util import general
from trident.util.notifications import notify
from dags.parking_meters.parking_meters_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
schedule = general.schedule['parking_meters']
start_date = general.start_date['parking_meters']
conf = general.config
cur_yr = general.get_year()

# This might need some refactoring (filenameing dates)

flist = {
    'full': 'treas_parking_payments_{}_datasd_v2.csv'.format(cur_yr),
    'by_month': 'treas_meters_{}_pole_by_month_datasd_v2.csv'.format(cur_yr),
    'by_day': 'treas_meters_{}_pole_by_mo_day_datasd_v2.csv'.format(cur_yr)
}

dag = DAG(
    dag_id='parking_meters',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule)

#: Downloads all parking files from FTP
get_parking_files = PythonOperator(
    task_id='get_parking_files',
    python_callable=download_latest,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Joins downloaded files from ftp to production
build_prod_file = PythonOperator(
    task_id='build_prod_file',
    python_callable=build_prod_file,
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Uploads the generated production file
upload_prod_file = S3FileTransferOperator(
    task_id='upload_parking_full',
    source_base_path=conf['prod_data_dir'],
    source_key=flist['full'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='parking_meters/' + flist['full'],
    replace=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Builds by month aggregation
build_by_month_aggregation = PythonOperator(
    task_id='build_by_month_agg',
    python_callable=build_aggregation,
    op_kwargs={'agg_type': 'pole_by_month'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Uploads the generated agg file
upload_by_month_agg = S3FileTransferOperator(
    task_id='upload_by_month_agg',
    source_base_path=conf['prod_data_dir'],
    source_key=flist['by_month'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='parking_meters/' + flist['by_month'],
    replace=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Builds by day aggregation
build_by_day_aggregation = PythonOperator(
    task_id='build_by_day_agg',
    python_callable=build_aggregation,
    op_kwargs={'agg_type': 'pole_by_mo_day'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Uploads the generated agg file
upload_by_day_agg = S3FileTransferOperator(
    task_id='upload_by_day_agg',
    source_base_path=conf['prod_data_dir'],
    source_key=flist['by_day'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='parking_meters/' + flist['by_day'],
    replace=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update data inventory json
update_json_date = PythonOperator(
    task_id='update_json_date',
    python_callable=update_json_date,
    provide_context=True,
    op_kwargs={'ds_fname': 'parking_meters_transactions'},
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_parking_trans_md = get_seaboard_update_dag('parking-meters-transactions.md', dag)

get_parking_files >> build_prod_file >> upload_prod_file >> [update_parking_trans_md,update_json_date]
build_prod_file >> [build_by_month_aggregation,build_by_day_aggregation]
build_by_month_aggregation >> upload_by_month_agg
build_by_day_aggregation >> upload_by_day_agg