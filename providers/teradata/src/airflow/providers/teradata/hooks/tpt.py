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
import secrets
import shutil
import string
import subprocess
import uuid
from tempfile import NamedTemporaryFile, TemporaryDirectory

from paramiko import SSHException

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook


def verify_tpt_utility_installed(utility: str):
    """Verify if a TPT utility (e.g., tbuild or tdload) is installed and available in the system's PATH."""
    if shutil.which(utility) is None:
        raise AirflowException(
            f"TPT utility '{utility}' is not installed or not available in the system's PATH"
        )


def verify_tpt_utility_on_remote_host(ssh_client, utility: str):
    """Verify if a TPT utility (tbuild/tdload) is installed on the remote host via SSH."""
    try:
        command = f"which {utility}"
        stdin, stdout, stderr = ssh_client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8").strip()
        error = stderr.read().decode("utf-8").strip()
        if exit_status != 0 or not output:
            raise AirflowException(
                f"TPT utility '{utility}' is not installed or not available in PATH on the remote host. "
                f"stderr: {error if error else 'N/A'}"
            )
    except Exception as e:
        raise AirflowException(f"Failed to verify TPT utility '{utility}' on remote host: {str(e)}")


def transfer_file_sftp(ssh_client, local_path, remote_path):
    sftp = ssh_client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()


def decrypt_file_remote(ssh_client, remote_encrypted_path, remote_decrypted_path, password):
    # Use process substitution to pass the password directly to openssl
    decryption_command = (
        f"echo '{password}' | openssl enc -aes-256-cbc -d -pbkdf2 -in {remote_encrypted_path} "
        f"-out {remote_decrypted_path} -pass stdin"
    )
    stdin, stdout, stderr = ssh_client.exec_command(decryption_command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status == 0:
        print("Decryption successful on remote server.")
    else:
        print(f"Decryption failed. Error: {stderr.read().decode()}")


def generate_random_password(length=12):
    # Define the character set: letters, digits, and special characters
    # Exclude problematic shell characters
    safe_punctuation = "".join(c for c in string.punctuation if c not in "'\"\\")
    characters = string.ascii_letters + string.digits + safe_punctuation
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


# def encrypt_file(file_path, password, output_path):
#     salt = os.urandom(16)
#     kdf = PBKDF2HMAC(
#         algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000, backend=default_backend()
#     )
#     key = kdf.derive(password.encode())
#     iv = os.urandom(16)
#     cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
#     encryptor = cipher.encryptor()
#     padder = PKCS7(algorithms.AES.block_size).padder()

#     with open(file_path, "rb") as f:
#         plaintext = f.read()
#     padded_data = padder.update(plaintext) + padder.finalize()
#     ciphertext = encryptor.update(padded_data) + encryptor.finalize()

#     with open(output_path, "wb") as f:
#         f.write(salt + iv + ciphertext)


def encrypt_file(file_path, password, output_path):
    subprocess.run(
        [
            "openssl",
            "enc",
            "-aes-256-cbc",
            "-salt",
            "-pbkdf2",
            "-in",
            file_path,
            "-out",
            output_path,
            "-k",
            password,
        ],
        check=True,
    )


class TptHook(TtuHook):
    """
    Hook for executing Teradata Parallel Transporter (TPT) operations.

    This hook extends BaseHook to provide interfaces for TPT data movement operations,
    including table-to-file exports, file-to-table loads, and table-to-table transfers.

    TPT is a high-performance data movement utility that can efficiently handle large volumes
    of data using parallel processing capabilities.

    Note: Requires Teradata Tools and Utilities (TTU) to be installed and properly configured
    on the system where this hook runs.

    Key Features:
    - Execute DDL operations using TPT
    - Export data from Teradata tables to files (table-to-file)
    - Load data into Teradata tables from files (file-to-table)
    - Transfer data between Teradata systems (table-to-table)

    Requirements:
    - TPT binaries (tbuild, tdload) must be installed and accessible in the system's PATH
    - Proper configuration of Teradata connection details in Airflow connections
    """

    def __init__(self, ssh_conn_id: str | None = None, *args, **kwargs):
        """Initialize the BteqHook by calling the parent's __init__ method."""
        super().__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id) if ssh_conn_id else None

    def execute_ddl(
        self,
        sql: str,
        error_list: list[int] | None = None,
        ssh_conn_id=None,
        working_dir: str | None = None,
    ) -> str | None:
        """
        Execute a DDL (Data Definition Language) statement using Teradata Parallel Transporter (TPT).

        This method prepares and executes a TPT script to perform DDL operations on a Teradata database.
        It supports error handling and logs the output of the TPT operation.

        :param sql: The DDL statement to execute.
        :param error_list: A list of integer error codes to handle during operation (defaults to empty list if None).
        :param ssh_conn_id: Connection ID for SSH tunnel if required.
        :param working_dir: Directory on remote host to use for temporary files (used only for SSH execution).
        :raises AirflowException: If the DDL operation fails or if invalid parameters are provided.
        :raises ValueError: If the SQL statement is empty.
        """
        if not sql or not sql.strip():
            raise ValueError("SQL statement cannot be empty")

        conn = self.get_conn()

        job_id = uuid.uuid4().hex
        with TemporaryDirectory(prefix="airflowtmp_ttu_tpt_") as tmp_dir:
            script_file = os.path.join(tmp_dir, f"tpt_script_{job_id}.txt")
            with open(script_file, "w", encoding="utf-8") as f:
                script_content = self._prepare_tpt_ddl_script(
                    sql, error_list, conn["host"], conn["login"], conn["password"]
                )
                f.write(script_content)
                f.flush()

            self.log.info("TPT script file created at %s", script_file)
            # Prepare the tbuild command
            tbuild_cmd = ["tbuild", "-f", script_file, f"airflow_tpt_{job_id}"]
            self.log.info("Executing tbuild command: %s", " ".join(tbuild_cmd))

            if ssh_conn_id:
                return self._execute_tbuild_via_ssh(tbuild_cmd, script_file, ssh_conn_id, working_dir)
            return self._execute_tbuild_locally(tbuild_cmd, conn)
        """
        Execute a DDL (Data Definition Language) statement using Teradata Parallel Transporter (TPT).

        This method prepares and executes a TPT script to perform DDL operations on a Teradata database.
        It supports error handling and logs the output of the TPT operation.

        :param sql: The DDL statement to execute.
        :param error_list: A list of integer error codes to handle during operation (defaults to empty list if None).
        :param ssh_conn_id: Connection ID for SSH tunnel if required.
        :raises AirflowException: If the DDL operation fails or if invalid parameters are provided.
        :raises ValueError: If the SQL statement is empty.
        """
        if not sql or not sql.strip():
            raise ValueError("SQL statement cannot be empty")

        conn = self.get_conn()

        job_id = uuid.uuid4().hex
        with TemporaryDirectory(prefix="airflowtmp_ttu_tpt_") as tmp_dir:
            script_file = os.path.join(tmp_dir, f"tpt_script_{job_id}.txt")
            with open(script_file, "w", encoding="utf-8") as f:
                script_content = self._prepare_tpt_ddl_script(
                    sql, error_list, conn["host"], conn["login"], conn["password"]
                )
                f.write(script_content)
                f.flush()

            self.log.info("TPT script file created at %s", script_file)
            # Prepare the tbuild command
            tbuild_cmd = ["tbuild", "-f", script_file, f"airflow_tpt_{job_id}"]
            self.log.info("Executing tbuild command: %s", " ".join(tbuild_cmd))

            if ssh_conn_id:
                return self._execute_tbuild_via_ssh(tbuild_cmd, script_file, ssh_conn_id)
            return self._execute_tbuild_locally(tbuild_cmd, conn)

    def _execute_tbuild_via_ssh(self, tbuild_cmd, script_file, ssh_conn_id, working_dir=None):
        """
        Execute tbuild command via SSH connection, with secure file transfer and cleanup, similar to _execute_tdload_via_ssh.

        :param tbuild_cmd: The tbuild command to execute
        :param script_file: Path to the TPT script file
        :param ssh_conn_id: SSH connection ID
        :param working_dir: Directory on remote host to use for temporary files
        :return: Exit status of the tbuild command
        """
        if not ssh_conn_id:
            raise ValueError("SSH connection ID must be provided for remote execution")

        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        local_script_file = script_file
        encrypt_file_path = f"{local_script_file}.enc"
        remote_dir = working_dir if working_dir else "/tmp"
        remote_encrypted_script_file = f"{remote_dir.rstrip('/')}/{os.path.basename(encrypt_file_path)}"
        remote_script_file = f"{remote_dir.rstrip('/')}/{os.path.basename(script_file)}"
        job_name = tbuild_cmd[-1]

        try:
            with ssh_hook.get_conn() as ssh_client:
                if not ssh_client:
                    raise AirflowException(f"Failed to establish SSH connection with {ssh_conn_id}")

                verify_tpt_utility_on_remote_host(ssh_client, "tbuild")

                try:
                    # Encrypt and transfer the script file
                    password = generate_random_password()
                    encrypt_file(local_script_file, password, encrypt_file_path)
                    transfer_file_sftp(ssh_client, encrypt_file_path, remote_encrypted_script_file)
                    decrypt_file_remote(
                        ssh_client, remote_encrypted_script_file, remote_script_file, password
                    )

                    # Update command to use remote script file path
                    remote_cmd = f"tbuild -f {remote_script_file} {job_name}"

                    self.log.info("Executing tbuild remotely via SSH on %s: %s", ssh_conn_id, remote_cmd)
                    _, stdout, stderr = ssh_client.exec_command(remote_cmd)
                    output = stdout.read().decode("utf-8")
                    error = stderr.read().decode("utf-8")
                    self.log.info("tbuild command stdout:\n%s", output)
                    exit_status = stdout.channel.recv_exit_status()
                    self.log.info("tbuild command exited with status %s", exit_status)

                    if exit_status != 0:
                        raise AirflowException(f"tbuild command failed with exit code {exit_status}: {error}")

                    return exit_status
                finally:
                    # Clean up local and remote files using shred for secure deletion
                    if os.path.exists(encrypt_file_path):
                        try:
                            subprocess.run(["shred", "--remove", encrypt_file_path], check=True)
                        except Exception:
                            os.remove(encrypt_file_path)
                    if os.path.exists(local_script_file):
                        try:
                            subprocess.run(["shred", "--remove", local_script_file], check=True)
                        except Exception:
                            os.remove(local_script_file)
                    # Use shred on remote files, fallback to rm if shred is not available
                    shred_cmd = (
                        f"shred --remove {remote_encrypted_script_file} {remote_script_file} "
                        f"|| rm -f {remote_encrypted_script_file} {remote_script_file}"
                    )
                    ssh_client.exec_command(shred_cmd)

        except SSHException as e:
            raise AirflowException(f"SSH connection error: {str(e)}")
        except Exception as e:
            raise AirflowException(f"Error executing tbuild command via SSH: {str(e)}")

    def _execute_tbuild_locally(self, tbuild_cmd, conn):
        """Execute tbuild command locally."""
        try:
            sp = subprocess.Popen(
                tbuild_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
            )
            conn["sp"] = sp

            error_line = (
                "An error occurred during the TPT operation. Please review the full TPT Output for details."
            )
            for line in iter(sp.stdout.readline, b""):
                decoded_line = line.decode(conn["console_output_encoding"]).strip()
                self.log.info(decoded_line)
                if "error" in decoded_line.lower():
                    error_line = decoded_line

            sp.wait()
            self.log.info("tbuild command exited with return code %s", sp.returncode)
            if sp.returncode:
                raise AirflowException(
                    f"TPT command exited with return code {sp.returncode} due to: {error_line}"
                )

            return sp.returncode
        finally:
            if "sp" in conn and conn["sp"] and conn["sp"].poll() is None:
                self.log.info("Terminating subprocess after execution.")
                conn["sp"].terminate()
                try:
                    conn["sp"].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log.warning("Subprocess did not terminate in time. Forcing kill.")
                    conn["sp"].kill()

    @staticmethod
    def _prepare_tpt_ddl_script(
        sql: str,
        error_list: list[int] | None,
        host: str,
        login: str,
        password: str,
        job_name: str | None = None,
    ) -> str:
        """
        Prepare a TPT script for executing a DDL statement.

        This method generates a TPT script that defines a DDL operator and applies the provided SQL statement.
        It also supports specifying a list of error codes to handle during the operation.

        :param sql: The DDL statement to execute.
        :param error_list: A list of error codes to handle during the operation.
        :param host: The Teradata host or TDPID (Teradata Directory Program Identifier).
        :param login: The username for logging into the Teradata database.
        :param password: The password for logging into the Teradata database.
        :param job_name: The name of the TPT job. Defaults to unique name if None.
        :return: A formatted TPT script as a string.
        :raises ValueError: If the SQL statement is empty.
        """
        if not sql or not sql.strip():
            raise ValueError("SQL statement cannot be empty")

        sql_statements = sql.split(";")
        # Replace single quotes with double single quotes in SQL statements
        sql_statements = [stmt.replace("'", "''") for stmt in sql_statements]

        apply_sql = ",\n".join(
            [
                f"('{stmt.strip()};')" if i == 0 else f"                ('{stmt.strip()};')"
                for i, stmt in enumerate(sql_statements)
                if stmt.strip()
            ]
        )
        if not apply_sql:
            raise ValueError("No valid SQL statements found in the provided input")
        # Remove the last comma from the apply_sql string
        apply_sql = apply_sql.rstrip(",")

        if job_name is None:
            job_name = f"airflow_tptddl_{uuid.uuid4().hex}"

        # Format error list for inclusion in the TPT script
        if not error_list:
            error_list_stmt = "VARCHAR ARRAY ErrorList = []"
        else:
            # error_list is expected to be a list of integers, e.g., [3706, 3803, 3807]
            # For the TPT script, these should be formatted as a list of quoted strings: ['3706', '3803', '3807']
            error_list_str = ", ".join([f"'{error}'" for error in error_list])
            error_list_stmt = f"VARCHAR ARRAY ErrorList = [{error_list_str}]"

        # Create and return the TPT script
        return f"""
            DEFINE JOB {job_name}
            DESCRIPTION 'TPT DDL Operation'
            (
            DEFINE OPERATOR DDL_Operator
            TYPE DDL
            ATTRIBUTES
            (
                VARCHAR TdpId = '{host}',
                VARCHAR UserName = '{login}',
                VARCHAR UserPassword = '{password}',
                {error_list_stmt}
            );

            APPLY
                {apply_sql}
            TO OPERATOR (DDL_Operator);
            );
        """

    def execute_tdload(
        self,
        mode: str,
        ssh_conn_id: str | None = None,
        source_table: str | None = None,
        select_stmt: str | None = None,
        target_table: str | None = None,
        source_file_name: str | None = None,
        target_file_name: str | None = None,
        source_format: str = "delimited",
        target_format: str = "delimited",
        source_text_delimiter: str = ",",
        target_text_delimiter: str = ",",
        staging_table: str | None = None,
        tdload_options: str | None = None,
        target_teradata_conn_id: str | None = None,
        working_dir: str | None = None,
    ) -> int:
        """
        Execute a tdload operation using the tdload command-line utility.

        This method directly executes tdload with appropriate parameters based on the operation mode.

        :param mode: The operation mode ('file_to_table', 'table_to_file', or 'table_to_table')
        :param ssh_conn_id: Connection ID for SSH tunnel if required
        :param source_table: Name of the source table (for table_to_file or table_to_table modes)
        :param select_stmt: SQL SELECT statement to retrieve data (alternative to source_table)
        :param target_table: Name of the target table (for file_to_table or table_to_table modes)
        :param source_file_name: Path to the source file (for file_to_table mode)
        :param target_file_name: Path to the target file (for table_to_file mode)
        :param source_format: Format of source data, defaults to "delimited"
        :param target_format: Format of target data, defaults to "delimited"
        :param source_text_delimiter: Source text delimiter, defaults to ","
        :param target_text_delimiter: Target text delimiter, defaults to ","
        :param staging_table: Staging table name if applicable
        :param tdload_options: Additional tdload options as a string or dict
        :param target_teradata_conn_id: Connection ID for target Teradata database
        :param working_dir: Directory on remote host to use for temporary files (used only for SSH execution)
        :return: None.
        :raises AirflowException: If the tdload command fails or returns a non-zero exit code.
        :raises ValueError: If required parameters are missing or invalid.
        """
        # Validate mode
        if not mode or mode not in ("file_to_table", "table_to_file", "table_to_table"):
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of 'file_to_table', 'table_to_file', or 'table_to_table'"
            )

        # Validate parameters based on mode
        if mode == "file_to_table" and (not source_file_name or not target_table):
            raise ValueError("file_to_table mode requires source_file_name and target_table")
        if mode == "table_to_file" and (not target_file_name or (not source_table and not select_stmt)):
            raise ValueError(
                "table_to_file mode requires target_file_name and either source_table or select_stmt"
            )
        if mode == "table_to_table" and (not target_table or (not source_table and not select_stmt)):
            raise ValueError(
                "table_to_table mode requires target_table and either source_table or select_stmt"
            )

        # Get source connection
        source_conn = self.get_conn()

        # Get target connection if it's different from source
        target_conn_dict = None
        if target_teradata_conn_id:
            target_conn = self.get_connection(target_teradata_conn_id)
            target_conn_dict = {
                "host": target_conn.host,
                "login": target_conn.login,
                "password": target_conn.password,
            }
        elif mode == "table_to_table":
            raise ValueError("table_to_table mode requires target_teradata_conn_id")

        # Create tdload job variable file based on the mode
        try:
            tdload_job_variable_file = self._prepare_tdload_script(
                mode=mode,
                source_table=source_table,
                select_stmt=select_stmt,
                target_table=target_table,
                source_file_name=source_file_name,
                target_file_name=target_file_name,
                source_format=source_format,
                target_format=target_format,
                source_text_delimiter=source_text_delimiter,
                target_text_delimiter=target_text_delimiter,
                staging_table=staging_table,
                source_conn=source_conn,
                target_conn=target_conn_dict,
            )
        except Exception as e:
            raise AirflowException(f"Failed to prepare tdload script: {e}")

        # Generate a unique job name
        job_name = f"airflow_tdload_{uuid.uuid4().hex}"

        # Prepare the tdload command
        tdload_cmd = ["tdload", "-j", tdload_job_variable_file]

        # Append tdload_options to the command if provided
        if tdload_options:
            for opt in tdload_options.split():
                tdload_cmd.append(opt)

        # Add the job name as the final argument
        tdload_cmd.append(job_name)

        # Log the command being executed
        self.log.info("Executing tdload command: %s", " ".join(tdload_cmd))

        # Execute the tdload command
        if ssh_conn_id:
            return self._execute_tdload_via_ssh(
                tdload_cmd, tdload_job_variable_file, ssh_conn_id, working_dir
            )
        return self._execute_tdload_locally(tdload_cmd, tdload_job_variable_file, source_conn)

    def _execute_tdload_via_ssh(self, tdload_cmd, job_variable_file, ssh_conn_id, working_dir=None) -> int:
        """
        Execute tdload command via SSH connection, with secure file transfer and cleanup, similar to _execute_tbuild_via_ssh.

        :param tdload_cmd: The tdload command to execute
        :param job_variable_file: Path to the job variable file
        :param ssh_conn_id: SSH connection ID
        :param working_dir: Directory on remote host to use for temporary files
        :return: Exit status of the tdload command
        """
        if not ssh_conn_id:
            raise ValueError("SSH connection ID must be provided for remote execution")

        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        local_job_file = job_variable_file
        encrypt_file_path = f"{local_job_file}.enc"
        remote_dir = working_dir if working_dir else "/tmp"
        remote_encrypted_job_file = f"{remote_dir.rstrip('/')}/{os.path.basename(encrypt_file_path)}"
        remote_job_file = f"{remote_dir.rstrip('/')}/{os.path.basename(job_variable_file)}"

        try:
            with ssh_hook.get_conn() as ssh_client:
                if not ssh_client:
                    raise AirflowException(f"Failed to establish SSH connection with {ssh_conn_id}")

                verify_tpt_utility_on_remote_host(ssh_client, "tdload")

                try:
                    # Encrypt and transfer the job variable file
                    password = generate_random_password()
                    encrypt_file(local_job_file, password, encrypt_file_path)
                    transfer_file_sftp(ssh_client, encrypt_file_path, remote_encrypted_job_file)
                    decrypt_file_remote(ssh_client, remote_encrypted_job_file, remote_job_file, password)

                    # Update command to use remote job variable file path
                    tdload_cmd[2] = remote_job_file
                    tdload_cmd_str = " ".join(tdload_cmd)

                    self.log.info("Executing tdload remotely via SSH on %s: %s", ssh_conn_id, tdload_cmd_str)
                    stdin, stdout, stderr = ssh_client.exec_command(tdload_cmd_str)
                    output = stdout.read().decode("utf-8")
                    error = stderr.read().decode("utf-8")
                    self.log.info("tdload command stdout:\n%s", output)
                    exit_status = stdout.channel.recv_exit_status()
                    self.log.info("tdload command exited with status %s", exit_status)

                    if exit_status != 0:
                        raise AirflowException(f"tdload command failed with exit code {exit_status}: {error}")

                    return exit_status
                finally:
                    # Clean up local and remote files using shred for secure deletion
                    if os.path.exists(encrypt_file_path):
                        try:
                            subprocess.run(["shred", "--remove", encrypt_file_path], check=True)
                        except Exception:
                            os.remove(encrypt_file_path)
                    if os.path.exists(local_job_file):
                        try:
                            subprocess.run(["shred", "--remove", local_job_file], check=True)
                        except Exception:
                            os.remove(local_job_file)
                    # Use shred on remote files, fallback to rm if shred is not available
                    shred_cmd = f"shred --remove {remote_encrypted_job_file} {remote_job_file} || rm -f {remote_encrypted_job_file} {remote_job_file}"
                    ssh_client.exec_command(shred_cmd)

        except SSHException as e:
            raise AirflowException(f"SSH connection error: {str(e)}")
        except Exception as e:
            raise AirflowException(f"Error executing tdload command via SSH: {str(e)}")

    def _execute_tdload_locally(self, tdload_cmd, tdload_job_variable_file, conn) -> int:
        """
        Execute tdload command locally.

        :param tdload_cmd: The tdload command to execute
        :param conn: Teradata connection dictionary
        :return: return code of the tdload command execution
        """
        sp = None
        try:
            sp = subprocess.Popen(
                tdload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid
            )
            conn["sp"] = sp

            # Process output
            error_lines = []
            if sp.stdout is not None:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode(conn["console_output_encoding"]).strip()
                    self.log.info(decoded_line)
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            sp.wait()
            self.log.info("tdload command exited with return code %s", sp.returncode)

            if sp.returncode != 0:
                error_msg = "\n".join(error_lines) if error_lines else "Unknown error"
                raise AirflowException(f"tdload command failed with return code {sp.returncode}: {error_msg}")

            return sp.returncode

        except Exception as e:
            raise AirflowException(f"Error executing tdload command: {str(e)}")
        finally:
            # Remove the job variable file
            if os.path.exists(tdload_job_variable_file):
                try:
                    os.remove(tdload_job_variable_file)
                    self.log.info("Removed job variable file: %s", tdload_job_variable_file)
                except OSError as e:
                    self.log.warning(
                        "Failed to remove job variable file %s: %s", tdload_job_variable_file, str(e)
                    )

            # Clean up the subprocess
            if sp and sp.poll() is None:
                self.log.info("Terminating subprocess after execution")
                try:
                    os.killpg(os.getpgid(sp.pid), 15)  # Send SIGTERM to process group
                    try:
                        sp.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.log.warning("Subprocess did not terminate in time. Forcing kill.")
                        os.killpg(os.getpgid(sp.pid), 9)  # Send SIGKILL to process group
                except (ProcessLookupError, OSError) as e:
                    self.log.warning("Error cleaning up subprocess: %s", str(e))

    def _prepare_tdload_script(
        self,
        mode: str,
        source_table: str | None,
        select_stmt: str | None,
        target_table: str | None,
        source_file_name: str | None,
        target_file_name: str | None,
        source_format: str,
        target_format: str,
        source_text_delimiter: str,
        target_text_delimiter: str,
        staging_table: str | None,
        source_conn: dict,
        target_conn: dict | None,
    ) -> str:
        """
        Prepare a tdload job variable file based on the specified mode.

        :param mode: The operation mode ('file_to_table', 'table_to_file', or 'table_to_table')
        :param source_table: Name of the source table
        :param select_stmt: SQL SELECT statement for data extraction
        :param target_table: Name of the target table
        :param source_file_name: Path to the source file
        :param target_file_name: Path to the target file
        :param source_format: Format of source data
        :param target_format: Format of target data
        :param source_text_delimiter: Source text delimiter
        :param target_text_delimiter: Target text delimiter
        :param staging_table: Staging table name if applicable
        :param source_conn: Source connection details dictionary
        :param target_conn: Target connection details dictionary
        :return: The path to the job variable file
        :raises ValueError: If invalid parameters are provided
        """
        # Create a dictionary to store job variables
        job_vars = {}

        # Add appropriate parameters based on the mode
        if mode == "file_to_table":
            job_vars.update(
                {
                    "TargetTdpId": source_conn["host"],
                    "TargetUserName": source_conn["login"],
                    "TargetUserPassword": source_conn["password"],
                    "TargetTable": target_table,
                    "SourceFileName": source_file_name,
                }
            )

        elif mode == "table_to_file":
            job_vars.update(
                {
                    "SourceTdpId": source_conn["host"],
                    "SourceUserName": source_conn["login"],
                    "SourceUserPassword": source_conn["password"],
                    "TargetFileName": target_file_name,
                }
            )

            if source_table:
                job_vars["SourceTable"] = source_table
            elif select_stmt:
                job_vars["SourceSelectStmt"] = select_stmt

        elif mode == "table_to_table":
            if not target_conn:
                raise ValueError("Target connection details required for table_to_table mode")

            job_vars.update(
                {
                    "SourceTdpId": source_conn["host"],
                    "SourceUserName": source_conn["login"],
                    "SourceUserPassword": source_conn["password"],
                    "TargetTdpId": target_conn["host"],
                    "TargetUserName": target_conn["login"],
                    "TargetUserPassword": target_conn["password"],
                    "TargetTable": target_table,
                }
            )

            if source_table:
                job_vars["SourceTable"] = source_table
            elif select_stmt:
                job_vars["SourceSelectStmt"] = select_stmt

        # Add common parameters if not empty
        if source_format:
            job_vars["SourceFormat"] = source_format
        if target_format:
            job_vars["TargetFormat"] = target_format
        if source_text_delimiter:
            job_vars["SourceTextDelimiter"] = source_text_delimiter
        if target_text_delimiter:
            job_vars["TargetTextDelimiter"] = target_text_delimiter
        if staging_table:
            job_vars["StagingTable"] = staging_table

        # Format job variables content
        job_var_content = "".join([f"{key}='{value}',\n" for key, value in job_vars.items()])
        job_var_content = job_var_content.rstrip(",\n")

        # Create a named temporary file that won't be automatically deleted
        with NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as var_file:
            var_file.write(job_var_content)
            var_file_path = var_file.name

        self.log.info("Created job variable file at %s.\n", var_file_path)

        return var_file_path
