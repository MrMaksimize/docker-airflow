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
from datetime import datetime
import logging
import json

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow import settings

from trident.util.notifications import send_email_swu

from trident.util.general import merge_dicts

from trident.util.general import config as conf
from airflow.models import Variable




class PoseidonEmailOperator(BaseOperator):
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
            to,
            subject,
            template_id=None,  #TODO replace with general template id
            dispatch_type='base_email_dispatch',
            template_data=None,
            dispatch_meta=None,
            cc=None,
            bcc=None,
            *args,
            **kwargs):
        super(PoseidonEmailOperator, self).__init__(*args, **kwargs)

        self.swu = {
            'to': to,
            'template_id': template_id,
            'subject': subject,
            'dispatch_type': dispatch_type or 'base_email_dispatch',
            'template_data': template_data or {},
            'dispatch_meta': dispatch_meta or {},
            'cc': cc,
            'bcc': bcc
        }



    def execute(self, context):
        contextual_meta = self.build_dispatch_meta(context)
        self.swu['dispatch_meta'] = merge_dicts(self.swu['dispatch_meta'], contextual_meta)
        send_email_swu(**self.swu)

    def build_dispatch_meta(self, context):
        return {
            'task_id': context['ti'].task_id,
            'dag_id': context['dag'].dag_id
        }



class PoseidonEmailWithPythonOperator(PoseidonEmailOperator):
    """
    Send provide data using a python callable
    """

    @apply_defaults
    def __init__(self,
        python_callable,
        op_args=None,
        op_kwargs=None,
        provide_context=True,
        *args, **kwargs):

        super(PoseidonEmailWithPythonOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context


    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
            self.op_kwargs = context

        # Contextual meta
        contextual_meta = self.build_dispatch_meta(context)

        self.swu['dispatch_meta'] = merge_dicts(self.swu['dispatch_meta'], contextual_meta)

        new_template_data = self.python_callable(*self.op_args, **self.op_kwargs)
        self.swu['template_data'] = merge_dicts(self.swu['template_data'], new_template_data)


        send_email_swu(**self.swu)



class PoseidonEmailFileUpdatedOperator(PoseidonEmailOperator):
    """
    Send last updated file
    """

    template_fields = ('file_bucket','to',)

    @apply_defaults
    def __init__(self,
        file_bucket,
        file_url,
        message = 'Hey there! Poseidon has updated your dataset!',
        *args,
        **kwargs):

        super(PoseidonEmailFileUpdatedOperator, self).__init__(*args, **kwargs)
        self.file_bucket = file_bucket
        self.file_url = file_url
        self.message = message


    def execute(self, context):
        # Contextual meta
        contextual_meta = self.build_dispatch_meta(context)

        self.swu['dispatch_meta'] = merge_dicts(self.swu['dispatch_meta'], contextual_meta)
        self.swu['template_data']['message'] = self.message
        if not self.file_bucket:
            self.swu['template_data']['file_url'] = self.file_url
        else: 
            self.swu['template_data']['file_url'] = 'http://'+self.file_bucket+self.file_url
        
        self.swu['dispatch_type'] = 'file_updated'
        self.swu['template_id'] = Variable.get("MAIL_SWU_FILE_UPDATED_TPL")

        send_email_swu(**self.swu)
        