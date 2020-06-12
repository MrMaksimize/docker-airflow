"""DSD permits _jobs file."""
#import os
import pandas as pd
import string
import numpy as np
from trident.util import general
from trident.util.geospatial import spatial_join_pt
import logging
from subprocess import Popen, PIPE
from shlex import quote

conf = general.config

def get_tags_file(**context):
    """ Get permit file from ftp site. """
    logging.info('Retrieving project tags from ftp.')

    exec_date = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Runs on Monday for data extracted Sunday
    file_date = exec_date.subtract(days=1)

    # Need zero-padded month and date
    filename = f"{file_date.year}" \
    f"{file_date.strftime('%m')}" \
    f"{file_date.strftime('%d')}"

    logging.info(f"Checking FTP for {filename}")

    fpath = f"P2K_261-Panda_Extract_DSD_Projects_Tags_{filename}.txt"

    command = f"cd {conf['temp_data_dir']} && " \
    f"curl --user {conf['ftp_datasd_user']}:{conf['ftp_datasd_pass']} " \
    f"-o {fpath} " \
    f"ftp://ftp.datasd.org/uploads/dsd/tags/" \
    f"{fpath} -sk"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        logging.info(f"Error with {fpath}")
        raise Exception(p.returncode)
    else:
        logging.info(f"Found {fpath}")

    return filename

def build_tags(**context):
    """Get PTS permits and create active and closed"""

    dtypes = {'DEVEL_NUM':'str',
    'PROJ_ID':'str',
    'PROJ_TAG_ID':'str'}

    filename = context['task_instance'].xcom_pull(dag_id="dsd_proj_tags",
        task_ids='get_tags_files')

    logging.info("Reading in project tag file")
    df = pd.read_csv(f"{conf['temp_data_dir']}/P2K_261-Panda_Extract_DSD_Projects_Tags_{filename}.txt",
        low_memory=False,
        sep=",",
        encoding="ISO-8859-1",
        dtype=dtypes)

    logging.info("File read successfully, renaming columns")

    df.columns = [x.lower() for x in df.columns]

    df = df.rename(columns={'devel_num':'development_id',
        'proj_id':'project_id',
        'proj_scope':'project_scope',
        'proj_tag_id':'project_tag_id',
        'description':'project_tag_desc'
        })

    logging.info("Writing prod file")
    general.pos_write_csv(
    df,
    f"{conf['prod_data_dir']}/permits_set1_project_tags_datasd.csv")

    return 'Created new project tags file'


