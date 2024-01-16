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

import os

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToTeradataOperator(BaseOperator):
    """
    Moves data from Teradata source database to Teradata destination database.
    .. seealso::
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TeradataToTeradataOperator`
    :param teradata_destination_conn_id: destination Teradata connection.
    :param destination_table: destination table to insert rows.
    :param teradata_source_conn_id: :ref:`Source Teradata connection <howto/connection:Teradata>`.
    :param source_sql: SQL query to execute against the source Teradata database
    :param source_sql_params: Parameters to use in sql query.
    :param rows_chunk: number of rows per chunk to commit.
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
        self.log.info("Loading %s to Teradata table %s...", self.s3_source_key, self.teradata_table)

        access_key = self.aws_access_key
        access_secret = self.aws_access_secret

        if not access_key or not access_secret:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            access_key = s3_hook.conn_config.aws_access_key_id
            access_secret = s3_hook.conn_config.aws_secret_access_key

        if access_key is None or access_secret is None:
            access_key = ""
            access_secret = ""

        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = f"""
                        CREATE MULTISET TABLE {self.teradata_table} AS
                        (
                            SELECT * FROM (
                                LOCATION = '{self.s3_source_key}'
                                ACCESS_ID= '{access_key}'
                                ACCESS_KEY= '{access_secret}'
                            ) AS d
                        ) WITH DATA
                        """
        self.log.info("COPYING using READ_NOS and CREATE TABLE AS feature of teradata....")
        self.log.info("sql : %s", sql)
        teradata_hook.run(sql)
        self.log.info("COPYING is completed")
