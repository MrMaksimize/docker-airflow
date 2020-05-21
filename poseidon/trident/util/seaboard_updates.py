"""Utilities for Updating Github Dates."""
import re, base64, os
import json
import logging
import random
from datetime import datetime
from github import Github

from airflow.operators.python_operator import PythonOperator

from trident.util import general

from trident.util.notifications import afsys_send_email


conf = general.config

def update_json_date(ds_fname, **kwargs):
    """ Take the dataset file name and update json date modified """

    repo_name = "COSD-PANDA/data-inventory"
    commit_branch = "master"
    exec_date = kwargs['next_execution_date'].in_tz(tz='US/Pacific').strftime("%Y-%m-%d")

    #: Auth to github
    tokens = conf["gh_tokens"]
    op_token = random.choice(tokens)
    gh = Github(login_or_token=op_token)

    #: Get repo and file
    repo = gh.get_repo(repo_name)
    inventory = repo.get_contents("data.json", commit_branch)
    file_contents = base64.b64decode(inventory.content).decode('utf-8')
    json_file = json.loads(file_contents)
    #: Find dataset and update date modified
    for ds in json_file['dataset']:
        if ds['identifier'] == ds_fname:
            modified = ds['modified']
            if modified == exec_date:
                return f"Date is already correct for {ds_fname}"
            else:
                ds['modified'] = exec_date

                new_json = json.dumps(json_file, indent=4)
                commit_msg = f"Date modified from {modified} to {exec_date} for {ds_fname}"

                outcome = repo.update_file(
                        path=inventory.path,
                        message=commit_msg,
                        content=new_json,
                        sha=inventory.sha,
                        branch=commit_branch)

                return commit_msg


def update_seaboard_date(ds_fname, **kwargs):
    repo_name = "COSD-PANDA/seaboard"
    fpath_pre = "src/_datasets/"
    commit_branch = "production"
    date_search_re = "(?<=date_modified\: \\\')\d{4}-\d{2}-\d{2}"
    exec_date = kwargs['next_execution_date'].in_tz(tz='US/Pacific').strftime("%Y-%m-%d")
    test_mode = kwargs['test_mode']

    #: Auth to github
    tokens = conf['gh_tokens']
    op_token = random.choice(tokens)
    gh = Github(login_or_token=op_token)

    #: Get repo
    repo = gh.get_repo(repo_name)

    logging.info('Looking for {}'.format(ds_fname))

    # Get file contents
    ds_file = repo.get_contents(fpath_pre + ds_fname, commit_branch)
    ds_file_content = base64.b64decode(ds_file.content)
    ds_file_decoded = ds_file_content.decode('utf-8')
    
    #df_file_content = ds_file.content

    #: Search for mod date in file
    match = re.search(date_search_re, ds_file_decoded)

    #: If no match, throw error
    if match is None:
        raise ValueError("No issued_date found in dataset")

    #: If not the same as exec date, update
    if match.group() == exec_date:
        return "{} already date is already correct {}".format(ds_file.name,
                                                              exec_date)
    else:
        updated_ds_file_content = re.sub(date_search_re, exec_date,
                                         ds_file_decoded, 1)
        commit_msg = "Poseidon: {} last updated {}, != {}.".format(
            ds_file.name, exec_date, match.group())

        logging.info("Updating {} from date {} to date {}".format(
            ds_file.name, match.group(), exec_date))

        # if test_mode is not True:
        repo.update_file(
            #path='/' + ds_file.path,
            path=ds_file.path,
            message=commit_msg,
            content=updated_ds_file_content,
            sha=ds_file.sha,
            branch=commit_branch)

        if commit_branch != 'master':
            repo.merge(
                base='master',
                head=commit_branch,
                commit_message='Pullback: ' + commit_msg)


        return commit_msg

def get_seaboard_update_dag(ds_fname, dag):
    task = PythonOperator(
        task_id='update_' + re.sub('-|\.', '_', ds_fname),
        python_callable=update_seaboard_date,
        provide_context=True,
        op_kwargs={'ds_fname': ds_fname},
        
        dag=dag)

    return task
