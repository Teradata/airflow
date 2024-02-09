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
"""An Airflow Hook for interacting with Teradata SQL Server."""
from __future__ import annotations

from typing import TYPE_CHECKING

import teradatasql
from teradatasql import TeradataConnection

from airflow.providers.common.sql.hooks.sql import DbApiHook


class TeradataHook(DbApiHook):
    """General hook for interacting with Teradata SQL Database.

    This module contains basic APIs to connect to and interact with Teradata SQL Database. It uses teradatasql
    client internally as a database driver for connecting to Teradata database. The config parameters like
    Teradata DB Server URL, username, password and database name are fetched from the predefined connection
    config connection_id. It raises an airflow error if the given connection id doesn't exist.

    You can also specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    .. seealso::
        - :ref:`Teradata API connection <howto/connection:teradata>`

    :param args: passed to DbApiHook
    :param database: The Teradata database to connect to.
    :param kwargs: passed to DbApiHook

    """

    # Override to provide the connection name.
    conn_name_attr = "teradata_conn_id"

    # Override to have a default connection id for a particular dbHook
    default_conn_name = "teradata_default"

    # Override if this db supports autocommit.
    supports_autocommit = True

    # Override this for hook to have a custom name in the UI selection
    conn_type = "teradata"

    # Override hook name to give descriptive name for hook
    hook_name = "Teradata"

    # Override with the Teradata specific placeholder parameter string used for insert queries
    placeholder: str = "?"

    # Override SQL query to be used for testing database connection
    _test_connection_sql = "select 1"

    def __init__(
        self,
        *args,
        database: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, schema=database, **kwargs)

    def get_conn(self) -> TeradataConnection:
        """Creates and returns a Teradata Connection object using teradatasql client.

        Establishes connection to a Teradata SQL database using config corresponding to teradata_conn_id.

        :return: a Teradata connection object
        """
        teradata_conn_config: dict = self._get_conn_config_teradatasql()
        teradata_conn = teradatasql.connect(**teradata_conn_config)
        return teradata_conn
