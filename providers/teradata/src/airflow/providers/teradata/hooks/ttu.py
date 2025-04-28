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

import os
import uuid
import subprocess
from typing import Any, Dict, Iterator, List, Optional, Union
from tempfile import gettempdir, NamedTemporaryFile, TemporaryDirectory

from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook

class TtuHook(BaseHook, LoggingMixin):
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
    conn_name_attr = 'ttu_conn_id'
    default_conn_name = 'ttu_default'
    conn_type = 'teradata'
    hook_name = 'Ttu'

    def __init__(self, ttu_conn_id: str = 'ttu_default') -> None:
        super().__init__()
        self.ttu_conn_id = ttu_conn_id
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn is not None:
            self.close_conn()

    def get_conn(self) -> dict:
        """
        Establishes and returns a connection dictionary with Teradata connection details.
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
                ttu_log_folder=extras.get('ttu_log_folder', '/tmp'),
                console_output_encoding=extras.get('console_output_encoding', 'utf-8'),
                bteq_session_encoding=extras.get('bteq_session_encoding', 'ASCII'),
                bteq_output_width=extras.get('bteq_output_width', 65531),
                bteq_quit_zero=extras.get('bteq_quit_zero', False),
                sp=None  # Subprocess placeholder
            )

            # Ensure log folder exists
            if not os.path.exists(self.conn['ttu_log_folder']):
                self.log.debug(f"Creating TTU log folder at {self.conn['ttu_log_folder']}")
                os.makedirs(self.conn['ttu_log_folder'], exist_ok=True)

        return self.conn

    def close_conn(self):
        """
        Closes the connection and cleans up any resources associated with it.
        Ensures that the subprocess, if running, is terminated gracefully.
        """
        if self.conn:
            if self.conn.get('sp') and self.conn['sp'].poll() is None:
                self.log.info("Terminating subprocess...")
                self.conn['sp'].terminate()
                try:
                    self.conn['sp'].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                    self.conn['sp'].kill()
            self.log.info("Closing TTU connection.")
            self.conn = None

    def execute_bteq(self, bteq_script: str, xcom_push_flag: bool = False) -> Optional[str]:
        """
        Executes BTEQ (Basic Teradata Query) sentences using the BTEQ binary.

        :param bteq_script: A string containing BTEQ sentences to execute.
        :param xcom_push_flag: If True, pushes the last line of the BTEQ log to XCom.
        :return: The last line of the BTEQ log if xcom_push_flag is True, otherwise None.
        :raises AirflowException: If the BTEQ command fails or returns a non-zero exit code.
        """
        # Establish connection and retrieve connection details
        conn = self.get_conn()
        self.log.info("Executing BTEQ script...")

        # Create a temporary directory for storing the BTEQ script
        with TemporaryDirectory(prefix='airflowtmp_ttu_bteq_') as tmp_dir:
            # Create a temporary file to write the BTEQ script
            with NamedTemporaryFile(dir=tmp_dir, mode='wb') as tmp_file:
                # Prepare the BTEQ script with connection parameters
                bteq_file_content = self._prepare_bteq_script(
                    bteq_string=bteq_script,
                    host=conn['host'],
                    login=conn['login'],
                    password=conn['password'],
                    bteq_output_width=conn['bteq_output_width'],
                    bteq_session_encoding=conn['bteq_session_encoding'],
                    bteq_quit_zero=conn['bteq_quit_zero']
                )
                self.log.debug("Generated BTEQ script:\n%s", bteq_file_content)

                # Write the BTEQ script to the temporary file
                tmp_file.write(bytes(bteq_file_content, 'UTF-8'))
                tmp_file.flush()
                tmp_file.seek(0)

                # Execute the BTEQ script using the BTEQ binary
                conn['sp'] = subprocess.Popen(
                    ['bteq'],
                    stdin=tmp_file,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir,
                    preexec_fn=os.setsid
                )

                # Capture and log the output of the BTEQ command
                last_line = ''
                failure_message = 'An error occurred during the BTEQ operation. Please review the full BTEQ output for details.'
                self.log.info("BTEQ Output:")
                for line in iter(conn['sp'].stdout.readline, b''):
                    decoded_line = line.decode(conn['console_output_encoding']).strip()
                    self.log.info(decoded_line)
                    last_line = decoded_line
                    if "Failure" in decoded_line:
                        # Save the last failure message
                        failure_message = decoded_line

                # Wait for the BTEQ process to complete
                conn['sp'].wait()
                self.log.info("BTEQ command exited with return code %s", conn['sp'].returncode)

                # Raise an exception if the BTEQ command failed
                if conn['sp'].returncode:
                    raise AirflowException(
                        f"BTEQ command exited with return code {conn['sp'].returncode} due to: {failure_message}"
                    )

                # Return the last line of the BTEQ log if xcom_push_flag is True
                if xcom_push_flag:
                    return last_line

    def on_kill(self):
        """
        Terminates the subprocess if it is running.
        Ensures that the process is terminated gracefully and logs the status.
        """
        self.log.debug('Attempting to kill child process...')
        conn = self.get_conn()
        if conn.get('sp'):
            try:
                self.log.info('Terminating subprocess...')
                conn['sp'].terminate()
                conn['sp'].wait(timeout=5)
                self.log.info('Subprocess terminated successfully.')
            except subprocess.TimeoutExpired:
                self.log.warning('Subprocess did not terminate in time. Forcing kill...')
                conn['sp'].kill()
                self.log.info('Subprocess killed forcefully.')
            except Exception as e:
                self.log.error(f'Failed to terminate subprocess: {e}')

    @staticmethod
    def _prepare_bteq_script(bteq_string: str, host: str, login: str, password: str, bteq_output_width: int, 
                             bteq_session_encoding: str, bteq_quit_zero: bool) -> str:
        """
        Prepare a BTEQ file with connection parameters for executing SQL sentences with BTEQ syntax.

        :param bteq_string: BTEQ sentences to execute.
        :param host: Teradata Host.
        :param login: Username for login.
        :param password: Password for login.
        :param bteq_output_width: Width of BTEQ output in the console.
        :param bteq_session_encoding: Session encoding. See official Teradata docs for possible values.
        :param bteq_quit_zero: If True, force a .QUIT 0 sentence at the end of the script (forcing return code = 0).
        :return: A formatted BTEQ script as a string.
        """
        if not bteq_string.strip():
            raise ValueError("BTEQ script cannot be empty.")

        # Construct the BTEQ script
        bteq_list = [
            f".LOGON {host}/{login},{password};",
            f".IF ERRORCODE <> 0 THEN .QUIT 8;",
            f".SET WIDTH {bteq_output_width};",
            f".SET SESSION CHARSET '{bteq_session_encoding}';",
            bteq_string.strip()
        ]

        # Add optional .QUIT 0 command if specified
        if bteq_quit_zero:
            bteq_list.append(".QUIT 0;")

        # Ensure proper termination of the script
        bteq_list.extend([".LOGOFF;", ".EXIT;"])

        # Join the script lines with newlines
        bteq_script = "\n".join(bteq_list)

        return bteq_script



