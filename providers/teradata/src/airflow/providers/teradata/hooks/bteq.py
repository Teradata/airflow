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
import shutil
import socket
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import TYPE_CHECKING

from paramiko import SSHException

if TYPE_CHECKING:
    from paramiko import SSHClient

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook


def verify_bteq_installed():
    """Verify if BTEQ is installed and available in the system's PATH."""
    if shutil.which("bteq") is None:
        raise AirflowException("BTEQ is not installed or not available in the system's PATH.")


def verify_bteq_installed_remote(ssh_client: SSHClient):
    """Verify if BTEQ is installed on the remote machine."""
    stdin, stdout, stderr = ssh_client.exec_command("which bteq")
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().strip()
    error = stderr.read().strip()

    if exit_status != 0 or not output:
        raise AirflowException(
            f"BTEQ is not installed or not available in PATH. stderr: {error.decode() if error else 'N/A'}"
        )


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
        """Execute the BTEQ script either in local machine or on remote host based on ssh_conn_id."""
        conn = self.get_conn()
        if not all([bteq_script.strip(), conn["host"], conn["login"], conn["password"]]):
            raise ValueError("Host, login, password, and BTEQ script must be provided.")
        if not isinstance(conn["bteq_output_width"], int) or conn["bteq_output_width"] <= 0:
            raise ValueError("Output width must be a positive integer.")
        if not conn["bteq_session_encoding"]:
            raise ValueError("Session encoding must be specified.")

        bteq_file_content = self._prepare_bteq_script(
            bteq_string=bteq_script,
            host=conn["host"],
            login=conn["login"],
            password=conn["password"],
            bteq_output_width=conn["bteq_output_width"],
            bteq_session_encoding=conn["bteq_session_encoding"],
            bteq_quit_zero=conn["bteq_quit_zero"],
            database=conn.get("database"),
        )
        # Remote execution
        if self.ssh_hook:
            return self._execute_bteq_remote(xcom_push_flag, timeout, bteq_file_content)
        return self._execute_bteq_local(xcom_push_flag, timeout, bteq_file_content, conn)

    def _execute_bteq_remote(
        self, xcom_push_flag: bool, timeout: int | None, bteq_file_content: str
    ) -> str | None:
        if not self.ssh_hook:
            raise AirflowException("SSHHook is not initialized. Please provide a valid `ssh_conn_id`.")
        local_script_path, remote_script_path = None, None
        try:
            with (
                self.ssh_hook.get_conn() as ssh_client,
                TemporaryDirectory(prefix="airflowtmp_ssh_bteq_") as tmp_dir,
            ):
                if ssh_client is None:
                    raise AirflowException("Failed to establish SSH connection. `ssh_client` is None.")

                verify_bteq_installed_remote(ssh_client)
                # Write script to local temp file
                local_script_path = os.path.join(tmp_dir, "bteq_script.txt")
                with open(local_script_path, "w") as script_file:
                    script_file.write(bteq_file_content)

                # Upload script to remote temp directory
                remote_script_path = os.path.join("/tmp", "bteq_script.txt")
                sftp_client = ssh_client.open_sftp()
                sftp_client.put(local_script_path, remote_script_path)
                sftp_client.close()
                command = f"bteq < {remote_script_path}"
                stdin, stdout, stderr = ssh_client.exec_command(command, timeout=timeout)
                last_line = ""
                failure_message = "An error occurred during the remote BTEQ operation."

                for line in stdout:
                    self.log.debug("Process stdout: ", line.strip())
                    last_line = line.strip()

                for line in stderr:
                    decoded_line = line.strip()
                    self.log.debug("Process stderr: ", line.strip())
                    if "Failure" in decoded_line:
                        failure_message = decoded_line

                if failure_message:
                    raise AirflowException(
                        f"BTEQ task failed on remote machine with error: {failure_message}"
                    )

                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    raise AirflowException(
                        f"BTEQ task failed on remote machine with error code: {exit_status}"
                    )

                return last_line if xcom_push_flag else None
        except (OSError, socket.gaierror):
            raise AirflowException(
                "SSH connection timed out. Please check the network or server availability."
            )
        except SSHException as e:
            raise AirflowException(f"SSH connection failed: {str(e)}")
        except Exception as e:
            raise AirflowException(f"An unexpected error occurred during SSH connection: {str(e)}")
        finally:
            # Remove the local script file
            if local_script_path and os.path.exists(local_script_path):
                os.remove(local_script_path)
            # Cleanup: Delete the remote temporary file
            if remote_script_path:
                cleanup_command = f"rm -f {remote_script_path}"
                ssh_client.exec_command(cleanup_command)

    def _execute_bteq_local(
        self, xcom_push_flag: bool, timeout: int | None, bteq_file_content: str, conn: dict
    ) -> str | None:
        verify_bteq_installed()
        with TemporaryDirectory(prefix="airflowtmp_ttu_bteq_") as tmp_dir:
            tmp_file_path = None
            try:
                with NamedTemporaryFile(dir=tmp_dir, mode="w+", delete=False) as tmp_file:
                    tmp_file.write(bteq_file_content)
                    tmp_file.flush()
                    tmp_file_path = tmp_file.name

                process = subprocess.Popen(
                    ["bteq"],
                    stdin=open(tmp_file.name, "rb"),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir,
                    preexec_fn=os.setsid,
                )

                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.on_kill()
                    raise AirflowException(f"BTEQ command timed out after {timeout} seconds.")

                conn["sp"] = process  # For `on_kill` support
                last_line = ""
                failure_message = "An error occurred during the BTEQ operation."

                if process.stdout is None:
                    raise AirflowException("Process stdout is None. Unable to read BTEQ output.")
                for line in iter(process.stdout.readline, b""):
                    try:
                        decoded_line = line.decode(conn["console_output_encoding"]).strip()
                        self.log.debug("Process output: %s", decoded_line)
                    except UnicodeDecodeError:
                        decoded_line = "<Decoding error: Non-UTF-8 output>"
                    last_line = decoded_line
                    if "Failure" in decoded_line:
                        failure_message = decoded_line

                if failure_message:
                    raise AirflowException(f"BTEQ task failed with error: {failure_message}")
                if process.returncode != 0:
                    raise AirflowException(f"BTEQ task failed with error code: {process.returncode}")

                return last_line if xcom_push_flag else None
            finally:
                if tmp_file_path and os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)

    def on_kill(self):
        """Terminate the subprocess if running."""
        self.log.debug("Attempting to kill child process...")
        conn = self.get_conn()
        process = conn.get("sp")
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                process.kill()
            except Exception as e:
                self.log.error("Failed to terminate subprocess: %s", str(e))

    def _prepare_bteq_script(
        self,
        bteq_string: str,
        host: str,
        login: str,
        password: str,
        bteq_output_width: int,
        bteq_session_encoding: str,
        bteq_quit_zero: bool,
        database: str | None = None,
    ) -> str:
        """Build a BTEQ script with necessary connection and session commands."""
        script_lines = [
            f".LOGON {host}/{login},{password}",
            ".IF ERRORCODE <> 0 THEN .QUIT 8",
            f".SET WIDTH {bteq_output_width}",
            f".SET SESSION CHARSET '{bteq_session_encoding}'",
        ]

        if database:
            script_lines.append(f"DATABASE {database};")

        script_lines.append(bteq_string.strip())

        if bteq_quit_zero:
            script_lines.append(".QUIT 0")

        script_lines.extend([".LOGOFF", ".EXIT"])
        self.log.debug("BTEQ Script: %s", script_lines)
        return "\n".join(script_lines)
