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

import boto3
from airflow.hooks.base_hook import BaseHook


class AwsHook(BaseHook):
    """
    Interact with AWS.

    This class is a thin wrapper around the boto3 python library.
    """
    def __init__(self, aws_conn_id='aws_default'):
        self.aws_conn_id = aws_conn_id

    def get_client_type(self, client_type):
        connection_object = self.get_connection(self.aws_conn_id)
        return boto3.client(
            client_type,
            region_name=connection_object.extra_dejson.get('region_name'),
            aws_access_key_id=connection_object.login,
            aws_secret_access_key=connection_object.password,
        )
