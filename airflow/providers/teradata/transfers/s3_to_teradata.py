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

import os
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToTeradataOperator(BaseOperator):
    """
    Used for transferring file contents from a file from S3 into a Teradata db table.

    :param s3_file_path: The path to the file (S3 key) that will be loaded into Teradata db.
    :param teradata_table: The Teradata database table into where the data will be loaded.
    :param aws_conn_id: The S3 connection that contains the credentials to the S3 Bucket.
    :param teradata_conn_id: Reference to :ref:`teradata connection id <howto/connection:teradata>`.
    """

    template_fields: Sequence[str] = (
        "s3_file_path",
        "teradata_table",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        s3_file_path: str,
        file_delimiter: str = ",",
        teradata_table: str,
        aws_conn_id: str = "aws_default",
        teradata_conn_id: str = "teradata_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_file_path = s3_file_path
        self.file_delimiter = file_delimiter
        self.teradata_table = teradata_table
        self.aws_conn_id = aws_conn_id
        self.teradata_conn_id = teradata_conn_id

    def execute(self, context: Context) -> None:
        """
        Executes the data transfer operation from file on AWS S3 storage to Teradata database.

        :param context: The context that is being provided when executing.
        """
        self.log.info("Loading %s to Teradata table %s...", self.s3_file_path, self.teradata_table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        file = s3_hook.download_file(key=self.s3_file_path)

        try:
            td = TeradataHook(teradata_conn_id=self.teradata_conn_id)
            td.bulk_load_file_into_table(
                table_name=self.teradata_table, src_file=file, file_delimiter=self.file_delimiter
            )
        finally:
            # Remove file downloaded from s3 to be idempotent.
            os.remove(file)
