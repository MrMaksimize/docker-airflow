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
from poseidon.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from poseidon.util import general


conf = general.config


class S3FileTransferOperator(BaseOperator):
    """
    Copies data from a source local location to S3.
    :param source_base_path: base path for local file
    :type source_base_path: str
    :param source_key: location of local file
    :type source_key: str
    param dest_s3_bucket: s3 bucket
    :type dest_s3_bucket: str
    :param dest_s3_conn_id: destination s3 connection
    :type dest_s3_conn_id: str
    :param dest_s3_key: The key to be written from S3
    :type dest_s3_key: str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool
    """

    ui_color = '#f9c915'

    @apply_defaults
    def __init__(self,
                 source_base_path,
                 source_key,
                 dest_s3_bucket,
                 dest_s3_conn_id='s3_default',
                 dest_s3_key=None,
                 replace=True,
                 use_gzip=False,
                 *args,
                 **kwargs):
        super(S3FileTransferOperator, self).__init__(*args, **kwargs)
        self.source_base_path = source_base_path
        self.source_key = source_key
        self.dest_s3_conn_id = dest_s3_conn_id
        self.dest_s3_bucket = dest_s3_bucket
        self.replace = replace
        self.use_gzip = use_gzip

        # Default to same path on aws if no path passed.
        if dest_s3_key == None:
            dest_s3_key = source_key
        self.dest_s3_key = dest_s3_key

    def execute(self, context):
        ti = context['ti']
        dest_s3 = S3Hook(s3_conn_id=self.dest_s3_conn_id)
        local_fpath = "%s/%s" % (self.source_base_path, self.source_key)
        logging.info("%s >>>>> %s/%s" %
                     (local_fpath, self.dest_s3_bucket, self.dest_s3_key))

        dest_s3.load_file(
            filename=local_fpath,
            key=self.dest_s3_key,
            bucket_name=self.dest_s3_bucket,
            #multipart_bytes=5242880,
            multipart_bytes=10000000,
            replace=self.replace,
            use_gzip=self.use_gzip)
        logging.info("Upload completed")

        dest_s3.connection.close()

        if conf['env'] == 'prod':
            url = "http://seshat.datasd.org/{}".format(self.dest_s3_key)
        else:
            url = "http://{}.s3.amazonaws.com/{}".format(self.dest_s3_bucket,
                                                         self.dest_s3_key)

        logging.info("URL: {}".format(url))

        self.verify_file_size_match(local_fpath, url)

        return url

    def verify_file_size_match(self, local_path, url):
        r = requests.head(url)
        r.raise_for_status()
        upload_size = int(r.headers['Content-Length'])
        local_size = int(os.path.getsize(local_path))
        assert upload_size == local_size, 'upload size {} does not match local size {}'.format(
            upload_size, local_size)

        logging.info("Upload size {} matches local size {}".format(upload_size,
                                                                   local_size))
