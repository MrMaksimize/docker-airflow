"""DSD Approvals _dags file."""
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from dags.dsd import dsd_approvals_jobs as app
from dags.dsd import dsdFileGetter as dfg
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

conf = general.config
args = general.args
schedule = general.schedule['dsd_approvals']
start_date = general.start_date['dsd_approvals']
dsd_temp_dir = general.create_path_if_not_exists(conf['temp_data_dir'] + '/')

fnames = [
    'curr_week_permits_completed', 'curr_week_permits_issued',
    'curr_week_apps_received'
]

#: Dag spec for dsd approvals
dag = DAG(dag_id='dsd_approvals',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule)

#: Latest Only Operator for dsd approvals.
dsd_approvals_latest_only = LatestOnlyOperator(
    task_id='dsd_approvals_latest_only', dag=dag)

#: Get most recent weekly permit approvals reports
get_approvals_files = PythonOperator(
    task_id='get_approvals_files',
    python_callable=dfg.get_files,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    op_kwargs={'fname_list': fnames,
               'target_dir': dsd_temp_dir},
    dag=dag)


#: dsd_approvals_latest_only must run before get_approvals_files
get_approvals_files.set_upstream(dsd_approvals_latest_only)

#: update github modified date (solar permits)
update_solar_md = get_seaboard_update_dag('solar-permits.md', dag)

for key in app.approval_dict:

    #: Consolidate weekly permitting data by scraping OpenDSD API
    scrape_dsd = PythonOperator(
        task_id='scrape_dsd_' + key,
        python_callable=app.scrape_dsd,
        op_kwargs={'key': key},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Add consolidated weekly data to current prod data
    update_dsd = PythonOperator(
        task_id='update_dsd_' + key,
        python_callable=app.update_dsd,
        op_kwargs={'key': key},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Extract solar permits
    extract_solar = PythonOperator(
        task_id='extract_solar_' + key,
        python_callable=app.extract_solar,
        op_kwargs={'key': key},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Upload data to S3
    upload_dsd = S3FileTransferOperator(
        task_id='upload_dsd_' + key,
        source_base_path=conf['prod_data_dir'],
        source_key=app.approval_dict[key][1],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_key='dsd/' + app.approval_dict[key][1],
        replace=True,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Upload solar permits
    upload_solar = S3FileTransferOperator(
        task_id='upload_solar_' + key,
        source_base_path=conf['prod_data_dir'],
        source_key='solar_permits_' + key + '_ytd_datasd.csv',
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_key='dsd/' + 'solar_permits_' + key + '_ytd_datasd.csv',
        replace=True,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: update github modified date (permits)
    update_md = get_seaboard_update_dag('permits-' + key + '.md', dag)

    #: Execution rules

    #: scrape_dsd tasks are executed after get_approvals_files task
    scrape_dsd.set_upstream(get_approvals_files)

    #: update_dsd tasks are executed after scrape_dsd tasks
    update_dsd.set_upstream(scrape_dsd)

    #: extract_solar tasks are executed after update_dsd tasks
    extract_solar.set_upstream(update_dsd)

    #: upload_dsd tasks are executed after update_dsd tasks
    upload_dsd.set_upstream(update_dsd)

    #: upload_solar tasks are executed after extract_solar tasks
    upload_solar.set_upstream(extract_solar)

    #: github updates are executed after S3 upload tasks
    update_md.set_upstream(upload_dsd)

    #: solar github updates are executed after S3 upload tasks
    update_solar_md.set_upstream(upload_solar)
