"""READ _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from trident.util import general
from dags.read.read_jobs import *
from trident.util.notifications import notify
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag, update_json_date

args = general.args
conf = general.config
schedule = general.schedule
start_date = general.start_date['read']

dag = DAG(dag_id='read',
    default_args=args,
    start_date=start_date,
    schedule_interval=schedule['read'],
    catchup=False
    )


#: Latest Only Operator for read
read_latest_only = LatestOnlyOperator(task_id='read_latest_only', dag=dag)


#: Retrieve READ billing data from FTP
get_billing = BashOperator(
    task_id='get_billing',
    bash_command=get_billing(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process billing data
process_billing = PythonOperator(
    task_id='process_billing',
    python_callable=process_billing,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload billing data to S3
billing_to_S3 = S3FileTransferOperator(
    task_id='billing_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key=datasd[0],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/' + datasd[0],
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Retrieve READ leases data from FTP
get_leases = BashOperator(
    task_id='get_leases',
    bash_command=get_leases(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process leases data
process_leases = PythonOperator(
    task_id='process_leases',
    python_callable=process_leases,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload leases data to S3
leases_to_S3 = S3FileTransferOperator(
    task_id='leases_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key=datasd[1],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/' + datasd[1],
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Retrieve READ leases data from FTP
get_parcels = BashOperator(
    task_id='get_parcels',
    bash_command=get_parcels(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process parcels data
process_parcels = PythonOperator(
    task_id='process_parcels',
    python_callable=process_parcels,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload parcels data to S3
parcels_to_S3 = S3FileTransferOperator(
    task_id='parcels_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key=datasd[2],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/' + datasd[2],
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Retrieve READ leases data from FTP
get_properties_details = BashOperator(
    task_id='get_properties_details',
    bash_command=get_properties_details(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Process properties details data
process_properties_details = PythonOperator(
    task_id='process_properties_details',
    python_callable=process_properties_details,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload properties details data to S3
properties_details_to_S3 = S3FileTransferOperator(
    task_id='properties_details_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key=datasd[3],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_key='read/' + datasd[3],
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

datasets = ['city_owned_properties','leases_city_owned_properties','city_owned_property_parcels']
s3_uploaders = [properties_details_to_S3,leases_to_S3,parcels_to_S3]

for index, dataset in enumerate(datasets):
    update_date_mod_json = PythonOperator(
        task_id=f"update_json_date_{dataset}",
        python_callable=update_json_date,
        provide_context=True,
        op_kwargs={'ds_fname': dataset},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: upload data must succeed before updating json
    update_date_mod_json.set_upstream(s3_uploaders[index])

#: Update leases portal modified date
update_leases_md = get_seaboard_update_dag('city-owned-properties-leases.md', dag)

#: Update details portal modified date
update_details_md = get_seaboard_update_dag('city-owned-properties-details.md', dag)

#: Update parcels portal modified date
update_parcels_md = get_seaboard_update_dag('city-owned-properties-parcels.md', dag)

#: Execution Rules

#: read_latest_only must run before get_billing
get_billing.set_upstream(read_latest_only)

#: read_latest_only must run before get_leases
get_leases.set_upstream(read_latest_only)

#: read_latest_only must run before get_parcels
get_parcels.set_upstream(read_latest_only)

#: read_latest_only must run before get_properties_details
get_properties_details.set_upstream(read_latest_only)

#: Billing data is processed after data retrieval.
process_billing.set_upstream(get_billing)

#: Leases data is processed after data retrieval.
process_leases.set_upstream(get_leases)

#: Parcels data is processed after data retireval.
process_parcels.set_upstream(get_parcels)

#: Properties details data is processed after data retrieval.
process_properties_details.set_upstream(get_properties_details)

#: Billing data is uploaded to S3 after being processed.
billing_to_S3.set_upstream(process_billing)

#: Leases data is uploaded to S3 after being processed.
leases_to_S3.set_upstream(process_leases)

#: Parcels data is uploaded to S3 after being processed.
parcels_to_S3.set_upstream(process_parcels)

#: Properties details data is uploaded to S3 after being processed.
properties_details_to_S3.set_upstream(process_properties_details)

#: Leases .md updated if leases upload to S3 succeeded.
update_leases_md.set_upstream(leases_to_S3)

#: Parcels .md updated if parcels upload to S3 succeeded.
update_parcels_md.set_upstream(parcels_to_S3)

#: Properties details .md updated if properties details upload to S3 succeeded.
update_details_md.set_upstream(properties_details_to_S3)
