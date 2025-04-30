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

import os
import subprocess
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class TtuHook(BaseHook):
    """
    Interact with Teradata using Teradata Tools and Utilities (TTU) binaries.

    This hook provides methods to execute BTEQ scripts, export data using TPT,
    load data into Teradata tables, and perform Teradata-to-Teradata data transfers.

    Note: It is required that TTU is previously installed and properly configured.

    Key Features:
    - Execute BTEQ scripts with connection parameters.
    - Export data from Teradata tables to files using TPT.
    - Load data into Teradata tables from files using TPT.
    - Perform Teradata-to-Teradata data transfers using TPT.

    Requirements:
    - TTU binaries (e.g., BTEQ, TPT) must be installed and accessible in the system's PATH.
    - Proper configuration of Teradata connection details in Airflow connections.

    Example Usage:
    ```python
    ttu_hook = TtuHook(ttu_conn_id="my_teradata_conn")
    ttu_hook.execute_bteq("SELECT * FROM my_table;")
    ```
    """

    conn_name_attr = "ttu_conn_id"
    default_conn_name = "ttu_default"
    conn_type = "teradata"
    hook_name = "Ttu"

    def __init__(self, ttu_conn_id: str = "ttu_default") -> None:
        super().__init__()
        self.ttu_conn_id = ttu_conn_id
        self.conn: dict[str, Any] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn is not None:
            self.close_conn()

    def get_conn(self) -> dict[str, Any]:
        """
        Establish and return a connection dictionary with Teradata connection details.

        If the connection is already established, it reuses the existing connection.

        :return: A dictionary containing connection details and configuration options.
        """
        if not self.conn:
            connection = self.get_connection(self.ttu_conn_id)
            extras = connection.extra_dejson

            # Validate required fields
            if not connection.login or not connection.password or not connection.host:
                raise AirflowException("Missing required connection parameters: login, password, or host.")

            # Set default values for optional extras
            self.conn = dict(
                login=connection.login,
                password=connection.password,
                host=connection.host,
                ttu_log_folder=extras.get("ttu_log_folder", "/tmp"),
                console_output_encoding=extras.get("console_output_encoding", "utf-8"),
                bteq_session_encoding=extras.get("bteq_session_encoding", "ASCII"),
                bteq_output_width=extras.get("bteq_output_width", 65531),
                bteq_quit_zero=extras.get("bteq_quit_zero", False),
                sp=None,  # Subprocess placeholder
            )
            # log the extras for debugging
            self.log.info("TTU connection extras: %s", extras)
            self.log.info("TTU connection details: %s", self.conn)

            # Ensure log folder exists
            if self.conn and not os.path.exists(self.conn["ttu_log_folder"]):
                self.log.debug("Creating TTU log folder at %s", self.conn["ttu_log_folder"])
                os.makedirs(self.conn["ttu_log_folder"], exist_ok=True)

        return self.conn

    def close_conn(self):
        """
        Close the connection and clean up any resources associated with it.

        Ensures that the subprocess, if running, is terminated gracefully.
        """
        if self.conn:
            if self.conn.get("sp") and self.conn["sp"].poll() is None:
                self.log.info("Terminating subprocess...")
                self.conn["sp"].terminate()
                try:
                    self.conn["sp"].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                    self.conn["sp"].kill()
            self.log.info("Closing TTU connection.")
            self.conn = None
