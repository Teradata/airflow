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
from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException


class AzureBlobStorageToTeradataOperator(BaseOperator):
    """

    Loads CSV, JSON and Parquet format data from Azure Blob Storage to Teradata.

    .. seealso::
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:AzureBlobStorageToTeradataOperator`

    :param blob_source_key: The object store URI with blob location. The URI format is /az/YOUR-STORAGE-ACCOUNT.blob.core.windows.net/YOUR-CONTAINER/YOUR-BLOB-LOCATION. Refer to
        https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
    :param azure_conn_id: The :ref:`Azure connection id<howto/connection:azure>`
        which refers to the information to connect to Azure service.
    :param teradata_table: destination table to insert rows.
    :param teradata_conn_id: :ref:`Teradata connection <howto/connection:Teradata>`
        which refers to the information to connect to Teradata

    """

    template_fields: Sequence[str] = ("blob_source_key", "teradata_table")
    template_fields_renderers = {"blob_source_key": "sql", "teradata_table": "py"}
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        blob_source_key: str,
        azure_conn_id: str = "azure_default",
        teradata_table: str,
        teradata_conn_id: str = "teradata_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.blob_source_key = blob_source_key
        self.azure_conn_id = azure_conn_id
        self.teradata_table = teradata_table
        self.teradata_conn_id = teradata_conn_id

    def execute(self, context: Context) -> None:
        """

        Execute the transfer operation from Azure Blob Storage to Teradata.

        :param context: The context that is being provided when executing.

        """
        azure_hook = WasbHook(wasb_conn_id=self.azure_conn_id)
        conn = azure_hook.get_connection(self.azure_conn_id)
        # Obtaining the Azure client ID and Azure secret in order to access a specified Blob container
        access_id = conn.login
        access_secret = conn.password
        # if no credentials, then accessing blob as public
        if access_id is None or access_secret is None:
            access_id = ""
            access_secret = ""

        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = """
                    CREATE MULTISET TABLE %s  AS
                    (
                        SELECT * FROM (
                            LOCATION = '%s'
                            ACCESS_ID= '%s'
                            ACCESS_KEY= '%s'
                    ) AS d
                    ) WITH DATA
                """ % (self.teradata_table, self.blob_source_key, access_id, access_secret)

        self.log.info("COPYING using READ_NOS and CREATE TABLE AS feature of teradata....")
        try:
            teradata_hook.run(sql, True)
        except Exception as ex:
            # Handling permission issue errors
            if "Error 3524" in str(ex):
                self.log.error("The user does not have CREATE TABLE access in teradata")
                raise
            if "Error 9134" in str(ex):
                self.log.error("There is an issue with the transfer operation. Please validate azure and "
                              "teradata connection details.")
                raise
            self.log.error("Issue occurred at Teradata: %s", str(ex))
            raise
        self.log.info("COPYING is completed")


