"""PD collisions _jobs file."""
import logging
import pandas as pd
from trident.util import general
from airflow.hooks.base_hook import BaseHook
from subprocess import Popen, PIPE
import subprocess
from shlex import quote

conf = general.config

def get_collisions_data(mode="activities",**context):
    """Download most recent collisions data from FTP."""
    exec_date = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Running this job at 5p should capture files for the day
    # the dag runs

    # File name does not have zero-padded numbers
    # But month is spelled, abbreviated
    # Pattern is month_day_year
    filename = f"{exec_date.strftime('%b')}_" \
    f"{exec_date.day}_" \
    f"{exec_date.year}"

    if mode == "activities":
        filetype = "Activity"
    elif mode == "details":
        filetype = "Person"
    else:
        raise Exception("Mode is invalid")

    # Collisions is misspelled
    fpath = f"Collissions_{filetype}_{filename}.csv"

    logging.info(f"Checking FTP for {fpath}")

    ftp_conn = BaseHook.get_connection(conn_id="FTP_DATASD")

    temp_dir = conf['temp_data_dir']

    command = f"cd {conf['temp_data_dir']} && " \
    f"curl --user {ftp_conn.login}:{ftp_conn.password} " \
    f"-o {fpath} " \
    f"ftp://ftp.datasd.org/uploads/sdpd/collisions/" \
    f"{fpath} -sk"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(p.returncode)
    else:
        logging.info("Found file")
        # return filename as context for next task
        return filename

def process_collisions_data(**context):
    """Process collision data."""
    #activity_date = context['task_instance'].xcom_pull(dag_id="pd_col.get_files",
        #task_ids='get_activities')
    #details_date = context['task_instance'].xcom_pull(dag_id="pd_col.get_files",
        #task_ids='get_activities')

    activity_date = "Jul_28_2020"
    details_date = "Jul_28_2020"
    
    if activity_date != details_date:

        raise Exception("Date of activity does not match date of details")

    else:
        logging.info("Reading in activity file")
        activity = pd.read_csv(f"{conf['temp_data_dir']}/Collissions_Activity_{activity_date}.csv",
            header=None,
            error_bad_lines=False,
            low_memory=False)
        logging.info("Reading in details file")
        details = pd.read_csv(f"{conf['temp_data_dir']}/Collissions_Person_{details_date}.csv",
            header=None,
            error_bad_lines=False,
            low_memory=False)

        activity.columns = ['report_id',
        'date_time',
        'injured',
        'killed',
        'hit_run_lvl',
        'police_beat',
        'address_no_primary',
        'address_pd_primary',
        'address_road_primary',
        'address_sfx_primary',
        'address_pd_intersecting',
        'address_name_intersecting',
        'address_sfx_intersecting',
        'violation_section',
        'violation_type',
        'charge_desc']

        details.columns = ['report_id',
        'date_time',
        'person_role',
        'person_injury_lvl',
        'person_veh_type',
        'veh_type',
        'veh_make',
        'veh_model']

    df = pd.merge(activity,details,how="inner",on="report_id")

    general.pos_write_csv(
        activity,
        f"{conf['prod_data_dir']}/pd_collisions_datasd_v1.csv",
        date_format="%Y-%m-%d %H:%M:%S")

    general.pos_write_csv(
        df,
        f"{conf['prod_data_dir']}/pd_collisions_details_datasd.csv",
        date_format="%Y-%m-%d %H:%M:%S")

    return 'Successfully processed collisions data.'
