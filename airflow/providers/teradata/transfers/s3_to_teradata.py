#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToTeradataOperator(BaseOperator):
    """
    Loads data from S3 file into a Teradata table.
    .. seealso::
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:S3ToTeradataOperator`
    :param s3_source_key: The path to the file (S3 key) that will be loaded into Teradata.
    :param teradata_table: destination table to insert rows.
    :param aws_conn_id: The S3 connection that contains the credentials to the S3 Bucket.
    :param teradata_conn_id: Reference to :ref:`teradata connection id <howto/connection:teradata>
    :param aws_access_key: S3 bucket access key.
    :param aws_access_secret: S3 bucket access secret.
    """

    template_fields: Sequence[str] = ("s3_source_key", "teradata_table")
    template_fields_renderers = {"s3_source_key": "sql", "teradata_table": "py"}
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        s3_source_key: str,
        teradata_table: str,
        aws_conn_id: str = "aws_default",
        teradata_conn_id: str = "teradata_default",
        aws_access_key: str = "",
        aws_access_secret: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_source_key = s3_source_key
        self.teradata_table = teradata_table
        self.aws_conn_id = aws_conn_id
        self.teradata_conn_id = teradata_conn_id
        self.aws_access_key = aws_access_key
        self.aws_access_secret = aws_access_secret

    def execute(self, context: Context) -> None:
        """
        Executes the transfer operation from S3 to Teradata.

        :param context: The context that is being provided when executing.
        """
        self.log.info("Loading %s to Teradata table %s...", self.s3_source_key, self.mysql_table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        access_key = s3_hook.conn_config.aws_access_key_id
        access_secret = s3_hook.conn_config.aws_secret_access_key
        self.log.info("access key : ", self.aws_access_key)
        self.log.info("access secret : ", self.aws_access_secret)
        if len(access_key) == 0 or len(access_secret) == 0:
            access_key = self.aws_access_key
            access_secret = self.aws_access_secret

        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = ("CREATE MULTISET TABLE %s AS ("
               "SELECT * FROM ( LOCATION = %s "
               "ACCESS_ID= %s ACCESS_KEY=%s ) AS d ) WITH DATA",
               self.teradata_table, self.s3_source_key, self.access_key, self.access_secret)
        self.log.info("sql : ", sql)
        teradata_hook.run(sql)

