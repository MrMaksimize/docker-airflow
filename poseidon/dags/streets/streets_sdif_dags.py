"""Streets sdif _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import notify

from dags.streets.streets_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['streets_sdif']
start_date = general.start_date['streets_sdif']

#: Dag spec
dag = DAG(dag_id='streets_sdif', default_args=args, start_date=start_date, schedule_interval=schedule)

#: Latest Only Operator for sdif
streets_latest_only = LatestOnlyOperator(task_id='sdif_latest_only', dag=dag)


#: Get streets data from DB
get_streets_data = PythonOperator(
    task_id='get_streets_data_sdif',
    python_callable=get_streets_paving_data,
    op_kwargs={'mode': 'sdif'},
    provide_context=True,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload prod streets file to S3
upload_streets_data = S3FileTransferOperator(
    task_id='upload_streets_data_sdif',
    source_base_path=conf['prod_data_dir'],
    source_key='sd_paving_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sd_paving_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)


for i in ['total', 'overlay', 'slurry']:

    sonar_task = PoseidonSonarCreator(
        task_id='create_sdif_{}_miles_paved_sonar'.format(i),
        range_id='days_30',
        value_key='sdif_{}_miles'.format(i),
        value_desc='Miles Paved {}'.format(i),
        python_callable=build_sonar_miles_aggs,
        op_kwargs={'mode': 'sdif',
                   'pav_type': i},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Depends on successful run of get_streets_data
    sonar_task.set_upstream(get_streets_data)



#: Update portal modified date
update_streets_md = get_seaboard_update_dag('streets-repair-projects.md', dag)

#: Execution order

#: streets_latest_only must run before get_streets_data
get_streets_data.set_upstream(streets_latest_only)

#: upload_streets_data is dependent on successful run of get_streets_data
upload_streets_data.set_upstream(get_streets_data)

#: update md file after upload to S3FileTransferOperator
update_streets_md.set_upstream(upload_streets_data)
