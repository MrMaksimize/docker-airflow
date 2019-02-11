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

from builtins import str
from datetime import datetime, timedelta
import logging
import json

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow import settings

from trident.util.general import merge_dicts

from trident.util.general import config as conf

from trident.util.notifications import notify_keen



def get_ranges(kwargs):
    today = kwargs['execution_date']
    return {
        'today': today,
        'days_7': today - timedelta(days=7),
        'days_30': today - timedelta(days=30)
    }


class PoseidonSonarCreator(BaseOperator):
    """
    :param to: comma separated string of email addresses
    :type to: string
    :param template_id:
    :type template_id: string
    :param dispatch_type:
    :type dispatch_type: string
    :param subject: email subject
    :type subject: string
    :param template_data:
    :type template_data: dict
    :param dispatch_meta
    :type dispatch_meta: dict
    :param cc:
    :type cc: string
    :param bcc:
    :type bcc: string
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            range_id,
            value_key,
            value_desc,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=True,
            *args, **kwargs):
        super(PoseidonSonarCreator, self).__init__(*args, **kwargs)

        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

        self.sonar = {
            'value_key': value_key,
            'value_desc': value_desc,
            'range_id': range_id
        }



    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
            self.op_kwargs = context

        # Contextual meta
        contextual_meta = self.build_sonar_meta(context)

        self.sonar = merge_dicts(self.sonar, contextual_meta)

        # Get range dt from meta
        range_start_dt = self.sonar['range_start']
        self.op_kwargs['range_start'] = range_start_dt
        # Pass it along to callable
        sonar_data = self.python_callable(*self.op_args, **self.op_kwargs)
        # Convert to string for json payload
        self.sonar['range_start'] = range_start_dt.strftime(conf[
            'date_format_keen'])
        self.sonar = merge_dicts(self.sonar, sonar_data)

        #print json.dumps(self.sonar, indent=4, sort_keys=True)

        notify_keen(
            self.sonar,
            'sonar_pings_{}'.format(conf['env']).lower(),
            raise_for_status=True)


    def build_sonar_meta(self, context):
        ranges = get_ranges(context)
        active_range = self.sonar['range_id']
        range_start = ranges[active_range]
        range_end = context['execution_date'].strftime(conf[
            'date_format_keen'])

        return {
            'task_id': context['ti'].task_id,
            'dag_id': context['dag'].dag_id,
            'exec_date': range_end,
            'range_start': range_start,
            'range_end': range_end
        }
