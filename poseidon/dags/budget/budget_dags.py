"""FM budget _dags file."""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from poseidon.operators.latest_only_operator import LatestOnlyOperator
from poseidon.dags.budget.budget_jobs import *
from poseidon.dags.budget.actuals_jobs import *
from poseidon.util import general
from poseidon.util.notifications import notify
from poseidon.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag
import os
import glob


args = general.args
conf = general.config
schedule = general.schedule['budget']
budget_fy = general.get_FY_short() + 1


dag = DAG(
    dag_id='budget',
    default_args=args,
    schedule_interval=schedule)


#: Latest Only Operator for budget
budget_latest_only = LatestOnlyOperator(
    task_id='budget_latest_only', dag=dag)

get_accounts = PythonOperator(
    task_id='get_chart_of_accounts',
    python_callable=get_accounts_chart,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_capital_ptd = PythonOperator(
    task_id='get_capital_ptd',
    python_callable=get_capital_ptd,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_capital_fy = PythonOperator(
    task_id='get_capital_fy',
    python_callable=get_capital,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_operating = PythonOperator(
    task_id='get_operating_fy',
    python_callable=get_operating,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_cip_ptd_act = PythonOperator(
    task_id='get_capital_ptd_act',
    python_callable=get_capital_ptd_act,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_cip_fy_act = PythonOperator(
    task_id='get_capital_act',
    python_callable=get_capital_act,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_op_act = PythonOperator(
    task_id='get_operating_act',
    python_callable=get_operating_act,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

get_refs = PythonOperator(
    task_id='get_reference_sets',
    python_callable=get_ref_sets,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

make_capital = PythonOperator(
    task_id='create_capital_sets',
    python_callable=create_capital,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

make_operating = PythonOperator(
    task_id='create_operating',
    python_callable=create_operating,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

make_cip_act = PythonOperator(
    task_id='create_capital_act',
    python_callable=create_capital_act,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

make_op_act = PythonOperator(
    task_id='create_operating_act',
    python_callable=create_operating_act,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Execution Rules

#: budget_latest_only must run before get_accounts
get_accounts.set_upstream(budget_latest_only)
#: get_accounts must run before get_refs
get_refs.set_upstream(get_accounts)

#: get_refs must run before get_capital_ptd
get_capital_ptd.set_upstream(get_refs)
#: get_refs must run before get_capital_fy
get_capital_fy.set_upstream(get_refs)
#: get_refs must run before get_operating
get_operating.set_upstream(get_refs)
#: get_refs must run before get_cip_ptd_act
get_cip_ptd_act.set_upstream(get_refs)
#: get_refs must run before get_cip_fy_act
get_cip_fy_act.set_upstream(get_refs)
#: get_refs must run before get_op_act
get_op_act.set_upstream(get_refs)

#: get_capital_fy must run before make_capital
make_capital.set_upstream(get_capital_fy)
#: get_operating must run before make_operating
make_operating.set_upstream(get_operating)
#: get_cip_fy_act must run before make_cip_act
make_cip_act.set_upstream(get_cip_fy_act)
#: get_op_act must run before make_op_act
make_op_act.set_upstream(get_op_act)

budget_files = [os.path.basename(x) for x in glob.glob(conf['prod_data_dir']+'/budget_*.csv')]
actuals_files = [os.path.basename(x) for x in glob.glob(conf['prod_data_dir']+'/actuals_*.csv')]

prod_files = budget_files + actuals_files
categories = ['_'.join(x.split('_')[0:2])
        for x in prod_files]

for index, f in enumerate(prod_files):

    cat = categories[index]

    #: Upload budget files to S3
    upload_task = S3FileTransferOperator(
        task_id='upload_'+f[0:-11],
        source_base_path=conf['prod_data_dir'],
        source_key=f,
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='budget/'+f,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)


    pos = [i for i, e in enumerate(categories) if e == cat]

    if cat == "budget_reference":
        ds_task = get_refs
        md = 'budget-reference-'+f.split('_')[2]

        #: Update portal modified date
        update_budget_md = get_seaboard_update_dag(md+'.md', dag)

        #: update md task must run after the upload task
        update_budget_md.set_upstream(upload_task)

    else:
        if cat == "budget_capital":
            ds_task = make_capital
            if 'ptd' in f:
                md = 'capital-budget-ptd'
            else:
                md = 'capital-budget-fy'
        elif cat == "budget_operating":
            ds_task = make_operating
            md = 'operating-budget'
        elif cat == "actuals_capital":
            ds_task = make_cip_act
            if 'ptd' in f:
                md = 'capital-actuals-ptd'
            else:
                md = 'capital-actuals-fy'
        elif cat == "actuals_operating":
            ds_task = make_op_act
            md = 'operating-actuals'
        else:
            ds_task = make_op_act
            md = 'operating-actuals'

        if pos:
            if index == pos[-1]:
                #: Update portal modified date
                update_budget_md = get_seaboard_update_dag(md+'.md', dag)

                #: update md task must run after the upload task
                update_budget_md.set_upstream(upload_task)


    #: make_operating must run after the get task
    upload_task.set_upstream(ds_task)