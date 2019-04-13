# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, logging, requests

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from trident.util import general


conf = general.config


class RScriptOperator(BashOperator):
    """
    Executes an R Script
    :param script_path: the full path of R script to execute
    :type script_path: str
    :param op_kwargs: the dictionary of keyword arguments to pass to the script
    :type op_kwargs: dict
    """

    ui_color = '#f9c915'

    @apply_defaults
    def __init__(self,
                 script_path,
                 op_kwargs=None,
                 *args,
                 **kwargs):
        self.script_call = script_path
        self.op_kwargs = op_kwargs
        if op_kwargs:
            for key, val in self.op_kwargs.items():
                self.script_call = "{} --{}={}".format(self.script_call, key, val)


        super(RScriptOperator, self).__init__(bash_command = self.script_call, *args, **kwargs)

    def execute(self, context):
        super(RScriptOperator, self).execute(context)
        return "Script Executed"



class RShinyDeployOperator(RScriptOperator):
    """
    Executes an R Script
    :param script_path: the full path of R script to execute
    :type script_path: str
    :param op_kwargs: the dictionary of keyword arguments to pass to the script
    :type op_kwargs: dict
    """

    ui_color = '#f9c915'

    @apply_defaults
    def __init__(self,
                 shiny_appname,
                 shiny_path,
                 shiny_acct_name,
                 shiny_token,
                 shiny_secret,
                 force = "TRUE",
                 *args,
                 **kwargs):

        self.shiny_appname = shiny_appname
        self.shiny_path = shiny_path
        self.shiny_acct_name = shiny_acct_name
        self.shiny_token = shiny_token
        self.shiny_secret = shiny_secret
        self.force = force

        super(RShinyDeployOperator, self).__init__(
                script_path="{}/poseidon/trident/util/shiny_deploy.R".format(conf['home_dir']),
                op_kwargs = {
                    "appname": self.shiny_appname,
                    "path": self.shiny_path,
                    "name": self.shiny_acct_name,
                    "token": self.shiny_token,
                    "secret": self.shiny_secret,
                    "force": self.force
                },
                *args,
                **kwargs
        )


    def execute(self, context):
        super(RShinyDeployOperator, self).execute(context)
        return "Script Executed"





