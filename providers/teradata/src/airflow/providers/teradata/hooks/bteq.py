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
from tempfile import NamedTemporaryFile, TemporaryDirectory

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook


class BteqHook(TtuHook):
    """
    Hook for executing BTEQ (Basic Teradata Query) scripts.

    This hook provides functionality to execute BTEQ scripts either locally or remotely via SSH.
    It extends the `TtuHook` and integrates with Airflow's SSHHook for remote execution.

    The BTEQ scripts are used to interact with Teradata databases, allowing users to perform
    operations such as querying, data manipulation, and administrative tasks.

    Features:
    - Supports both local and remote execution of BTEQ scripts.
    - Handles connection details, script preparation, and execution.
    - Provides robust error handling and logging for debugging.
    - Allows configuration of session parameters like output width and encoding.

    .. seealso::
        - :ref:`Teradata API connection <howto/connection:teradata>`

    :param teradata_conn_id: Reference to a specific Teradata connection.
    :param ssh_conn_id: Optional SSH connection ID for remote execution.
    :param args: Additional arguments passed to the parent `TtuHook`.
    :param kwargs: Additional keyword arguments passed to the parent `TtuHook`.
    """

    def __init__(self, ssh_conn_id: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id) if ssh_conn_id else None

    def execute_bteq(
        self, bteq_script: str, xcom_push_flag: bool = False, timeout: int | None = None
    ) -> str | None:
        """Execute the BTEQ script."""
        if self.ssh_hook:
            return self._execute_bteq_remote(bteq_script, xcom_push_flag, timeout)
        return self._execute_bteq_local(bteq_script, xcom_push_flag, timeout)

    def _execute_bteq_remote(self, bteq_script: str, xcom_push_flag: bool, timeout: int | None) -> str | None:
        conn = self.get_conn()
        self.log.info("Executing BTEQ script remotely via SSH...")

        bteq_file_content = self._prepare_bteq_script(
            bteq_string=bteq_script,
            host=conn["host"],
            login=conn["login"],
            password=conn["password"],
            bteq_output_width=conn["bteq_output_width"],
            bteq_session_encoding=conn["bteq_session_encoding"],
            bteq_quit_zero=conn["bteq_quit_zero"],
        )

        if self.ssh_hook:
            with (
                self.ssh_hook.get_conn() as ssh_client,
                TemporaryDirectory(prefix="airflowtmp_ssh_bteq_") as tmp_dir,
            ):
                local_script_path = os.path.join(tmp_dir, "bteq_script.txt")
                with open(local_script_path, "w") as script_file:
                    script_file.write(bteq_file_content)

                import tempfile

                remote_script_path = tempfile.gettempdir() + "/bteq_script.txt"
                sftp_client = ssh_client.open_sftp()
                sftp_client.put(local_script_path, remote_script_path)
                sftp_client.close()

                command = f"bteq < {remote_script_path}"
                stdin, stdout, stderr = ssh_client.exec_command(command, timeout=timeout)

                last_line = ""
                failure_message = "An error occurred during the remote BTEQ operation."
                self.log.info("Remote BTEQ Output:")
                for line in stdout:
                    decoded_line = line.strip()
                    self.log.info(decoded_line)
                    last_line = decoded_line
                    if "Failure" in decoded_line:
                        failure_message = decoded_line

                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    raise AirflowException(f"Remote BTEQ failed (exit {exit_status}): {failure_message}")

                return last_line if xcom_push_flag else None
        else:
            raise AirflowException("SSH connection is not established. Cannot execute BTEQ script remotely.")

    def _execute_bteq_local(self, bteq_script: str, xcom_push_flag: bool, timeout: int | None) -> str | None:
        conn = self.get_conn()
        self.log.info("Executing BTEQ script locally...")

        with TemporaryDirectory(prefix="airflowtmp_ttu_bteq_") as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, mode="w+", delete=False) as tmp_file:
                bteq_file_content = self._prepare_bteq_script(
                    bteq_string=bteq_script,
                    host=conn["host"],
                    login=conn["login"],
                    password=conn["password"],
                    bteq_output_width=conn["bteq_output_width"],
                    bteq_session_encoding=conn["bteq_session_encoding"],
                    bteq_quit_zero=conn["bteq_quit_zero"],
                )
                self.log.debug("Generated BTEQ script:\n%s", bteq_file_content)
                tmp_file.write(bteq_file_content)
                tmp_file.flush()

                process = subprocess.Popen(
                    ["bteq"],
                    stdin=open(tmp_file.name, "rb"),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir,
                    preexec_fn=os.setsid,
                )

                conn["sp"] = process  # Store reference for `on_kill`
                last_line = ""
                failure_message = "An error occurred during the BTEQ operation."

                if process.stdout is None:
                    raise AirflowException("Process stdout is None. Unable to read output.")

                self.log.info("BTEQ Output:")
                for line in iter(process.stdout.readline, b""):
                    decoded_line = line.decode(conn["console_output_encoding"]).strip()
                    self.log.info(decoded_line)
                    last_line = decoded_line
                    if "Failure" in decoded_line:
                        failure_message = decoded_line

                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.on_kill()
                    raise AirflowException(f"BTEQ command timed out after {timeout} seconds.")

                if process.returncode != 0:
                    raise AirflowException(
                        f"BTEQ exited with return code {process.returncode}: {failure_message}"
                    )

                return last_line if xcom_push_flag else None

    def on_kill(self):
        """Terminate the subprocess if running."""
        self.log.debug("Attempting to kill child process...")
        conn = self.get_conn()
        process = conn.get("sp")
        if process:
            try:
                self.log.info("Terminating subprocess...")
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                process.kill()
            except Exception as e:
                self.log.error("Failed to terminate subprocess: %s", str(e))

    @staticmethod
    def _prepare_bteq_script(
        bteq_string: str,
        host: str,
        login: str,
        password: str,
        bteq_output_width: int,
        bteq_session_encoding: str,
        bteq_quit_zero: bool,
    ) -> str:
        """Build a BTEQ script with necessary connection and session commands."""
        if not all([bteq_string.strip(), host, login, password]):
            raise ValueError("Host, login, password, and BTEQ script must be provided.")
        if not isinstance(bteq_output_width, int) or bteq_output_width <= 0:
            raise ValueError("Output width must be a positive integer.")
        if not bteq_session_encoding:
            raise ValueError("Session encoding must be specified.")

        script_lines = [
            f".LOGON {host}/{login},{password};",
            ".IF ERRORCODE <> 0 THEN .QUIT 8;",
            f".SET WIDTH {bteq_output_width};",
            f".SET SESSION CHARSET '{bteq_session_encoding}';",
            bteq_string.strip(),
        ]

        if bteq_quit_zero:
            script_lines.append(".QUIT 0;")

        script_lines.extend([".LOGOFF;", ".EXIT;"])
        return "\n".join(script_lines)
