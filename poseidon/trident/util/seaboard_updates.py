"""Utilities for Updating Github Dates."""
import re, base64, os
import logging
import random
from datetime import datetime
from github import Github

from airflow.operators.python_operator import PythonOperator

from trident.util import general

from trident.util.notifications import notify


conf = general.config

def update_seaboard_date(ds_fname, **kwargs):
    repo_name = "cityofsandiego/seaboard"
    fpath_pre = "src/_datasets/"
    commit_branch = "production"
    date_search_re = "(?<=date_modified\: \\\')\d{4}-\d{2}-\d{2}"
    exec_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    test_mode = kwargs['test_mode']

    #: Auth to github
    tokens = conf['gh_tokens']
    op_token = random.choice(tokens)
    gh = Github(login_or_token=op_token)

    #: Get repo
    repo = gh.get_repo(repo_name)

    logging.info('Looking for {}'.format(ds_fname))

    # Get file contents
    ds_file = repo.get_file_contents(fpath_pre + ds_fname, commit_branch)
    ds_file_content = base64.b64decode(ds_file.content)

    #: Search for mod date in file
    match = re.search(date_search_re, ds_file_content)

    #: If no match, throw error
    if match is None:
        raise ValueError("No issued_date found in dataset")

    #: If not the same as exec date, update
    if match.group() == exec_date:
        return "{} already date is already correct {}".format(ds_file.name,
                                                              exec_date)
    else:
        updated_ds_file_content = re.sub(date_search_re, exec_date,
                                         ds_file_content, 1)
        commit_msg = "Poseidon: {} last updated {}, != {}.".format(
            ds_file.name, exec_date, match.group())

        logging.info("Updating {} from date {} to date {}".format(
            ds_file.name, match.group(), exec_date))

        # if test_mode is not True:
        repo.update_file(
            path='/' + ds_file.path,
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
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    return task
