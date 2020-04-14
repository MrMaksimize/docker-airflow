"""TSW Integration _dags file."""
import re
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email


from dags.tsw_integration.tsw_integration_jobs import *

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['tsw_integration']
start_date = general.start_date['tsw_integration']

#: Dag spec
dag = DAG(dag_id='tsw_integration',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule,
    catchup=False
    )

# VPM Extraction Support Tasks


#: Download VPM dump from FTP
get_vpm_violations = BashOperator(
    task_id='get_vpm_violations',
    bash_command=get_vpm_violations_wget(),
    on_failure_callback=afsys_send_email,
    dag=dag)


#: Download VPM dump from FTP
#get_vpm_dump = BashOperator(
#    task_id='get_vpm_dump',
#    bash_command=ftp_download_wget(),
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)
#
#
##: Extract VPM dump
#extract_vpm_dump = BashOperator(
#    task_id='extract_vpm_dump',
#    bash_command=get_tar_command(),
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)
#
#
##: Drop MySQL VPM
#drop_vpm_temp_db = MySqlOperator(
#    task_id='drop_vpm_temp_db',
#    mysql_conn_id='VPM_TEMP',
#    sql='DROP DATABASE IF EXISTS vpm_temp',
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)
#
#
##: Create MySQL VPM
#create_vpm_temp_db = MySqlOperator(
#    task_id='create_vpm_temp_db',
#    mysql_conn_id='VPM_TEMP',
#    sql='CREATE DATABASE vpm_temp',
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)
#
#
###: Populate MySQL VPM
#populate_vpm_temp_db = MySqlOperator(
#    task_id='populate_vpm_temp_db',
#    mysql_conn_id='VPM_TEMP',
#    sql=get_vpm_populate_sql(),
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)




#: get_vpm_violations, process, output file
#get_vpm_violations = PythonOperator(
#    task_id='get_vpm_violations',
#    python_callable=get_vpm_violations,
#    on_failure_callback=afsys_send_email,
#    
#    
#    dag=dag)

# END VPM Extraction Support Tasks


#: get_sf_violations, process, output file
get_sf_violations = PythonOperator(
    task_id='get_sf_violations',
    python_callable=get_sf_violations,
    on_failure_callback=afsys_send_email,
    dag=dag)


#: get_pts_violations, process, output file
#get_pts_violations = PythonOperator(
    #task_id='get_pts_violations',
    #python_callable=get_pts_violations,
    #on_failure_callback=afsys_send_email,
    #
    #
    #dag=dag)

get_pts_violations = BashOperator(
    task_id='get_pts_violations',
    bash_command=get_pts_violations(),
    on_failure_callback=afsys_send_email,
    dag=dag)


#: combine violations, process, output file
combine_sw_violations = PythonOperator(
    task_id='combine_sw_violations',
    python_callable=combine_violations,
    on_failure_callback=afsys_send_email,
    dag=dag)



#: Upload prod csv file to S3
violations_csv_to_s3 = S3FileTransferOperator(
    task_id='violations_csv_to_s3',
    source_base_path=conf['prod_data_dir'],
    source_key='stormwater_violations_merged.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw_int/stormwater_violations_merged.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)


#: Upload prod csv with null geos file to S3
violations_csv_null_geos_to_s3 = S3FileTransferOperator(
    task_id='violations_csv_w_null_geos_to_s3',
    source_base_path=conf['prod_data_dir'],
    source_key='stormwater_violations_merged_null_geos.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw_int/stormwater_violations_merged_null_geos.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)


#: Upload prod geojson file to S3
violations_geojson_to_s3 = S3FileTransferOperator(
    task_id='violations_geojson_to_s3',
    source_base_path=conf['prod_data_dir'],
    source_key='stormwater_violations_merged.geojson',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw_int/stormwater_violations_merged.geojson',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)


addresses_to_S3 = S3FileTransferOperator(
    task_id='upload_address_book',
    source_base_path=conf['prod_data_dir'],
    source_key='sw_viols_address_book.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['ref_s3_bucket'],
    dest_s3_key='sw_viols_address_book.csv',
    on_failure_callback=afsys_send_email,
    replace=True,
    dag=dag)

#: Execution rules
[get_vpm_violations,get_sf_violations,get_pts_violations] >> combine_sw_violations
combine_sw_violations >> [violations_csv_to_s3,violations_geojson_to_s3,violations_csv_null_geos_to_s3,addresses_to_S3]