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
import socket
import subprocess
import tempfile
from contextlib import contextmanager

from paramiko import SSHException

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.providers.teradata.utils.bteq_util import (
    prepare_bteq_command_for_local_execution,
    prepare_bteq_command_for_remote_execution,
    transfer_file_sftp,
    verify_bteq_installed,
    verify_bteq_installed_remote,
)
from airflow.providers.teradata.utils.encryption_utils import (
    decrypt_remote_file_to_string,
    generate_encrypted_file_with_openssl,
    generate_random_password,
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
        - :ref:`hook API connection <howto/connection:teradata>`

    :param teradata_conn_id: Reference to a specific Teradata connection.
    :param ssh_conn_id: Optional SSH connection ID for remote execution.
    :param bteq_input: The BTEQ script to be executed. (templated)
    :param bteq_session_encoding: The encoding for the BTEQ session.
    :param bteq_script_encoding: The encoding for the BTEQ script.
    :param bteq_quit_rc: The return code for the BTEQ quit command.
    :param timeout: The timeout for the BTEQ execution.
    """

    def __init__(self, ssh_conn_id: str | None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id) if ssh_conn_id else None

    def execute_bteq_script(
        self,
        bteq_script: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: int,
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
    ) -> int | None:
        """Execute the BTEQ script either in local machine or on remote host based on ssh_conn_id."""
        # Remote execution
        if self.ssh_hook:
            # Write script to local temp file
            # Encrypt the file locally
            return self.execute_bteq_script_at_remote(
                bteq_script,
                remote_working_dir,
                bteq_script_encoding,
                timeout,
                timeout_rc,
                bteq_session_encoding,
                bteq_quit_rc,
            )
        return self.execute_bteq_script_at_local(
            bteq_script, bteq_script_encoding, timeout, timeout_rc, bteq_quit_rc, bteq_session_encoding
        )

    def execute_bteq_script_at_remote(
        self,
        bteq_script: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: int,
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
    ) -> int | None:
        with (
            self.preferred_temp_directory() as tmp_dir,
        ):
            # Preparing lo
            file_path = os.path.join(tmp_dir, "bteq_script.txt")
            with open(file_path, "w", encoding=str(bteq_script_encoding or "UTF-8")) as f:
                f.write(bteq_script)
            return self._transfer_to_and_execute_bteq_on_remote(
                file_path,
                remote_working_dir,
                bteq_script_encoding,
                timeout,
                timeout_rc,
                bteq_quit_rc,
                bteq_session_encoding,
                tmp_dir,
            )

    def _transfer_to_and_execute_bteq_on_remote(
        self,
        file_path: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: int,
        timeout_rc: int | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        bteq_session_encoding: str | None,
        tmp_dir: str,
    ) -> int | None:
        encrypted_file_path = None
        remote_encrypted_path = None
        try:
            if self.ssh_hook and self.ssh_hook.get_conn():
                with self.ssh_hook.get_conn() as ssh_client:
                    if ssh_client is None:
                        raise AirflowException("Failed to establish SSH connection. `ssh_client` is None.")
                    verify_bteq_installed_remote(ssh_client)
                    password = generate_random_password()  # Encryption/Decryption password
                    encrypted_file_path = os.path.join(tmp_dir, "bteq_script.enc")
                    generate_encrypted_file_with_openssl(file_path, password, encrypted_file_path)
                    remote_encrypted_path = os.path.join(remote_working_dir or "", "bteq_script.enc")

                    transfer_file_sftp(ssh_client, encrypted_file_path, remote_encrypted_path)

                    bteq_command_str = prepare_bteq_command_for_remote_execution(
                        timeout=timeout,
                        bteq_script_encoding=bteq_script_encoding or "",
                        bteq_session_encoding=bteq_session_encoding or "",
                        timeout_rc=timeout_rc or -1,
                    )

                    exit_status, stdout, stderr = decrypt_remote_file_to_string(
                        ssh_client,
                        remote_encrypted_path,
                        password,
                        bteq_command_str,
                    )

                    failure_message = None

                    for line in stderr:
                        decoded_line = line.strip()
                        self.log.debug("Process stderr: ", line.strip())
                        if "Failure" in decoded_line:
                            failure_message = decoded_line
                    self.log.info("BTEQ command executed with exit status: %s", exit_status)
                    # Raising an exception if there is any failure in bteq and also user wants to fail the
                    # task otherwise just log the error message as warning to not fail the task.
                    if (
                        failure_message
                        and exit_status != 0
                        and exit_status
                        not in (
                            bteq_quit_rc
                            if isinstance(bteq_quit_rc, (list, tuple))
                            else [bteq_quit_rc if bteq_quit_rc is not None else 0]
                        )
                    ):
                        raise AirflowException(f"BTEQ task failed with error: {failure_message}")
                    if failure_message:
                        self.log.warning(failure_message)
                    return exit_status
            else:
                raise AirflowException("SSH connection is not established. `ssh_hook` is None or invalid.")
        except (OSError, socket.gaierror):
            raise AirflowException(
                "SSH connection timed out. Please check the network or server availability."
            )
        except SSHException as e:
            raise AirflowException(f"An unexpected error occurred during SSH connection: {str(e)}")
        except AirflowException as e:
            raise e
        except Exception as e:
            raise AirflowException(
                f"An unexpected error occurred while executing BTEQ script on remote machine: {str(e)}"
            )
        finally:
            # Remove the local script file
            if encrypted_file_path and os.path.exists(encrypted_file_path):
                os.remove(encrypted_file_path)
            # Cleanup: Delete the remote temporary file
            if encrypted_file_path:
                cleanup_en_command = f"rm -f {remote_encrypted_path}"
                if self.ssh_hook and self.ssh_hook.get_conn():
                    with self.ssh_hook.get_conn() as ssh_client:
                        if ssh_client is None:
                            raise AirflowException(
                                "Failed to establish SSH connection. `ssh_client` is None."
                            )
                        ssh_client.exec_command(cleanup_en_command)

    def execute_bteq_script_at_local(
        self,
        bteq_script: str,
        bteq_script_encoding: str | None,
        timeout: int,
        timeout_rc: int | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        bteq_session_encoding: str | None,
    ) -> int | None:
        verify_bteq_installed()
        bteq_command_str = prepare_bteq_command_for_local_execution(
            self.get_conn(),
            timeout=timeout,
            bteq_script_encoding=bteq_script_encoding or "",
            bteq_session_encoding=bteq_session_encoding or "",
            timeout_rc=timeout_rc or -1,
        )
        self.log.info("Executing BTEQ command after adding logon details: %s", bteq_command_str)
        process = subprocess.Popen(
            bteq_command_str,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            preexec_fn=os.setsid,
        )
        encode_bteq_script = bteq_script.encode(str(bteq_script_encoding or "UTF-8"))
        stdout_data, _ = process.communicate(input=encode_bteq_script)
        try:
            # https://docs.python.org/3.10/library/subprocess.html#subprocess.Popen.wait  timeout is in seconds
            process.wait(timeout=timeout + 60)  # Adding 1 minute extra for BTEQ script timeout
        except subprocess.TimeoutExpired:
            self.on_kill()
            raise AirflowException(f"BTEQ command timed out after {timeout} seconds.")
        conn = self.get_conn()
        conn["sp"] = process  # For `on_kill` support
        failure_message = None
        self.log.info("Bteq output is : %s", stdout_data)
        if stdout_data is None:
            raise AirflowException("Process stdout is None. Unable to read BTEQ output.")
        decoded_line = ""
        for line in stdout_data.splitlines():
            try:
                decoded_line = line.decode(str(bteq_script_encoding or "UTF-8")).strip()
                self.log.debug("Process output: %s", decoded_line)
            except UnicodeDecodeError:
                self.log.warning("Failed to decode line: %s", line)
            if "Failure" in decoded_line:
                failure_message = decoded_line
        self.log.info("BTEQ command executed with exit status: %s", process.returncode)
        # Raising an exception if there is any failure in bteq and also user wants to fail the
        # task otherwise just log the error message as warning to not fail the task.
        if (
            failure_message
            and process.returncode != 0
            and process.returncode
            not in (
                bteq_quit_rc
                if isinstance(bteq_quit_rc, (list, tuple))
                else [bteq_quit_rc if bteq_quit_rc is not None else 0]
            )
        ):
            raise AirflowException(f"BTEQ task failed with error: {failure_message}")
        if failure_message:
            self.log.warning(failure_message)

        return process.returncode

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

    def get_airflow_home_dir(self) -> str:
        """Get the AIRFLOW_HOME directory."""
        return os.environ.get("AIRFLOW_HOME", "~/airflow")

    @contextmanager
    def preferred_temp_directory(self, prefix="bteq_"):
        try:
            temp_dir = tempfile.gettempdir()
            if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
                raise OSError("OS temp dir not usable")
        except Exception:
            temp_dir = self.get_airflow_home_dir()

        with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
            yield tmp
