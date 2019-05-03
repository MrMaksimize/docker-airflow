"""PW Facilities_dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from poseidon.dags.public_works.pw_fac_jobs import *
from poseidon.util import general
from poseidon.util.access_helpers import *
from poseidon.util.notifications import notify
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

args = general.args
conf = general.config
schedule = general.schedule

dag = DAG(dag_id='pw_fac',
          default_args=args,
          schedule_interval=schedule['pw_fac'])


#: Latest Only Operator for pw_fac
pw_fac_latest_only = LatestOnlyOperator(
    task_id='pw_fac_latest_only', dag=dag)

#: Get facilities data from FTP and save to temp folder
get_occupied_fac_data = BashOperator(
    task_id='get_occupied_fac_data',
    bash_command=get_fac_data(
        'GF\ Facilities\ FCA\ Alpha/GF\ City\ Occupied\ Facilities\ FY\ 14\ to\ 16.accdb',
        conf['temp_data_dir'] + '/city_occupied_gf.accdb'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


extract_occupied_fac_campus_attr = BashOperator(
    task_id='extract_occupied_fac_campus_attr',
    bash_command=extract_access_to_csv(
        conf['temp_data_dir'] + '/city_occupied_gf.accdb', 'CampusAttributes',
        conf['temp_data_dir'] + '/city_occupied_gf_campus_attr.csv'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


extract_occupied_fac_campus_forecasts = BashOperator(
    task_id='extract_occupied_fac_campus_forecasts',
    bash_command=extract_access_to_csv(
        conf['temp_data_dir'] + '/city_occupied_gf.accdb', 'CampusForecasts',
        conf['temp_data_dir'] + '/city_occupied_gf_campus_forecasts.csv'),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)


#: Process collisions data and save result to prod folder
process_occupied_fac_data = PythonOperator(
    task_id='process_occupied_fac_data',
    python_callable=process_occupied_fac_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod file to S3
occupied_fac_to_s3 = S3FileTransferOperator(
    task_id='occupied_fac_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='gf_city_occupied_fac_datasd.csv',
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='public_works/gf_city_occupied_fac_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Update portal modified date
update_fac_md = get_seaboard_update_dag('facilities-assessment.md', dag)

#: Execution rules:

#: pd_col_latest_only must run before get_collisions_data
get_occupied_fac_data.set_upstream(pw_fac_latest_only)

#: Data Extraction begins after data retrieval
extract_occupied_fac_campus_attr.set_upstream(get_occupied_fac_data)
extract_occupied_fac_campus_forecasts.set_upstream(get_occupied_fac_data)

#: Data processing is triggered after data extraction.
process_occupied_fac_data.set_upstream(extract_occupied_fac_campus_attr)
process_occupied_fac_data.set_upstream(extract_occupied_fac_campus_forecasts)

#: Data upload to S3 is triggered after data processing completion.
occupied_fac_to_s3.set_upstream(process_occupied_fac_data)

#: Github update depends on S3 upload success.
update_fac_md.set_upstream(occupied_fac_to_s3)
