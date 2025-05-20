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
import uuid
from tempfile import NamedTemporaryFile, TemporaryDirectory

from airflow.exceptions import AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook


class TptHook(TtuHook):
    """
    Hook for executing Teradata Parallel Transporter (TPT) operations.

    This hook extends BaseHook to provide interfaces for TPT data movement operations,
    including table-to-file exports, file-to-table loads, and table-to-table transfers.

    TPT is a high-performance data movement utility that can efficiently handle large volumes
    of data using parallel processing capabilities.

    Note: Requires Teradata Tools and Utilities (TTU) to be installed and properly configured
    on the system where this hook runs.

    Extras example:
    ```
    {"ttu_log_folder": "/path/to/logs", "console_output_encoding": "utf-8"}
    ```

    Key Features:
    - Execute DDL operations using TPT
    - Export data from Teradata tables to files (table-to-file)
    - Load data into Teradata tables from files (file-to-table)
    - Transfer data between Teradata systems (table-to-table)

    Requirements:
    - TPT binaries (tbuild, tdload) must be installed and accessible in the system's PATH
    - Proper configuration of Teradata connection details in Airflow connections
    """

    def __init__(self, *args, **kwargs):
        """Initialize the BteqHook by calling the parent's __init__ method."""
        super().__init__(*args, **kwargs)

    def execute_ddl(
        self, sql: str, error_list=None, xcom_push_flag: bool = False, ssh_conn_id=None
    ) -> str | None:
        """
        Execute a DDL (Data Definition Language) statement using Teradata Parallel Transporter (TPT).

        This method prepares and executes a TPT script to perform DDL operations on a Teradata database.
        It supports error handling and logs the output of the TPT operation.

        :param sql: The DDL statement to execute.
        :param error_list: A list of error codes to handle during operation (defaults to empty list if None).
        :param xcom_push_flag: Whether to push the result of the operation to XCom. Defaults to False.
        :param ssh_conn_id: Connection ID for SSH tunnel if required.
        :return: The last line of the TPT log if xcom_push_flag is True, otherwise None.
        :raises AirflowException: If the DDL operation fails or if invalid parameters are provided.
        :raises ValueError: If the SQL statement is empty.
        """
        if not sql or not sql.strip():
            raise ValueError("SQL statement cannot be empty")

        # Default to empty list if None
        if error_list is None:
            error_list = []

        conn = self.get_conn()
        tbuild_logs_dir = os.path.join(conn["ttu_log_folder"], "tbuild", "logs")
        tbuild_checkpoint_dir = os.path.join(conn["ttu_log_folder"], "tbuild", "checkpoint")

        # Create directories if they don't exist
        for directory in [tbuild_logs_dir, tbuild_checkpoint_dir]:
            os.makedirs(directory, exist_ok=True)

        job_id = uuid.uuid4().hex
        with TemporaryDirectory(prefix="airflowtmp_ttu_tpt_") as tmp_dir:
            script_file = os.path.join(tmp_dir, f"tpt_script_{job_id}.txt")
            with open(script_file, "w", encoding="utf-8") as f:
                script_content = self._prepare_tpt_ddl_script(
                    sql, error_list, conn["host"], conn["login"], conn["password"]
                )
                f.write(script_content)
                f.flush()

            self.log.debug("Temporary TPT Template location: %s", script_file)
            self.log.info("TPT Template content:\n%s", script_content)

            tbuild_cmd = ["tbuild", "-f", script_file, f"airflow_tpt_{job_id}"]

            if ssh_conn_id:
                return self._execute_tbuild_via_ssh(tbuild_cmd, script_file, ssh_conn_id, xcom_push_flag)
            return self._execute_tbuild_locally(tbuild_cmd, conn, xcom_push_flag)

    def _execute_tbuild_via_ssh(self, tbuild_cmd, script_file, ssh_conn_id, xcom_push_flag):
        """
        Execute tbuild command via SSH connection.

        :param tbuild_cmd: The tbuild command to execute
        :param script_file: Path to the TPT script file
        :param ssh_conn_id: SSH connection ID
        :param xcom_push_flag: Whether to push the result to XCom
        :return: Command result if xcom_push_flag is True, otherwise None
        """
        remote_script_file = f"/tmp/airflow_tpt_{os.path.basename(script_file)}"
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        try:
            with ssh_hook.get_conn() as ssh_client:
                # Copy the TPT script file to the remote server via SFTP
                self.log.info("Copying TPT script to remote server via SFTP: %s", script_file)
                sftp_client = ssh_client.open_sftp()
                try:
                    sftp_client.put(script_file, remote_script_file)
                finally:
                    sftp_client.close()
                self.log.info("TPT script copied to remote server at %s", remote_script_file)

                # Update command to use remote script file path
                tbuild_cmd[2] = remote_script_file
                tbuild_cmd_str = " ".join(tbuild_cmd)

                self.log.info("Executing tbuild remotely via SSH on %s: %s", ssh_conn_id, tbuild_cmd_str)
                stdin, stdout, stderr = ssh_client.exec_command(tbuild_cmd_str)
                output = stdout.read().decode("utf-8")
                error = stderr.read().decode("utf-8")
                self.log.info("tbuild command stdout:\n%s", output)
                if error:
                    self.log.error("tbuild command stderr:\n%s", error)
                exit_status = stdout.channel.recv_exit_status()
                self.log.info("tbuild command exited with status %s", exit_status)

                if exit_status != 0:
                    raise AirflowException(f"tbuild command failed with exit code {exit_status}: {error}")

                if xcom_push_flag and output:
                    return output.strip().splitlines()[-1] if output.strip() else "0"
                return None

        except Exception as e:
            self.log.error("Error executing tbuild command via SSH: %s", str(e))
            raise AirflowException(f"Error executing tbuild command via SSH: {str(e)}")
        finally:
            try:
                with ssh_hook.get_conn() as ssh_client:
                    sftp_client = ssh_client.open_sftp()
                    try:
                        sftp_client.remove(remote_script_file)
                        self.log.info("Remote TPT script file cleaned up: %s", remote_script_file)
                    finally:
                        sftp_client.close()
            except Exception as cleanup_error:
                self.log.warning(
                    "Failed to clean up remote file %s: %s", remote_script_file, str(cleanup_error)
                )

    def _execute_tbuild_locally(self, tbuild_cmd, conn, xcom_push_flag):
        """Execute tbuild command locally."""
        try:
            sp = subprocess.Popen(
                tbuild_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
            )
            conn["sp"] = sp

            last_line = ""
            error_line = (
                "An error occurred during the TPT operation. Please review the full TPT Output for details."
            )
            for line in iter(sp.stdout.readline, b""):
                decoded_line = line.decode(conn["console_output_encoding"]).strip()
                self.log.info(decoded_line)
                last_line = decoded_line
                if "error" in decoded_line.lower():
                    error_line = decoded_line

            sp.wait()
            self.log.info("tbuild command exited with return code %s", sp.returncode)
            if sp.returncode:
                raise AirflowException(
                    f"TPT command exited with return code {sp.returncode} due to: {error_line}"
                )

            if xcom_push_flag:
                return last_line
            return None
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
        sql: str, error_list, host: str, login: str, password: str, job_name: str | None = None
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

        if job_name is None:
            job_name = f"airflow_tptddl_{uuid.uuid4().hex}"

        # Format error list for inclusion in the TPT script
        if not error_list:
            error_list_stmt = "VARCHAR ARRAY ErrorList = [],"
        else:
            # Handle both string and list input formats
            if isinstance(error_list, str):
                try:
                    # Clean the string and split by commas
                    error_codes = error_list.replace("'", "").replace('"', "").split(",")
                except Exception as e:
                    raise ValueError(f"Failed to parse error_list string: {e}")
            else:
                # Assume it's already a list/iterable
                error_codes = error_list

            # Format each error code
            formatted_errors = [f"'{str(error).strip()}'" for error in error_codes]
            formatted_error_list = ", ".join(formatted_errors)
            error_list_stmt = f"VARCHAR ARRAY ErrorList = [{formatted_error_list}],"

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
                VARCHAR TraceLevel = 'None'
            );

            APPLY
                {sql}
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
        tdload_options: str | dict | None = None,
        target_teradata_conn_id: str | None = None,
        xcom_push_flag: bool = False,
    ) -> str | None:
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
        :param xcom_push_flag: Whether to push the result of the operation to XCom. Defaults to False.
        :return: The last line of the tdload output if xcom_push_flag is True, otherwise None.
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
                tdload_options=tdload_options,
                source_conn=source_conn,
                target_conn=target_conn_dict,
            )
        except Exception as e:
            raise AirflowException(f"Failed to prepare tdload script: {e}")

        # Create necessary directories for logs
        tdload_logs_dir = os.path.join(source_conn["ttu_log_folder"], "tdload", "logs")
        os.makedirs(tdload_logs_dir, exist_ok=True)

        # Generate a unique job name
        job_name = f"airflow_tdload_{uuid.uuid4().hex}"

        # Prepare the tdload command
        tdload_cmd = ["tdload", "-j", tdload_job_variable_file]

        # Append tdload_options to the command if provided
        if tdload_options:
            if isinstance(tdload_options, str):
                # Split tdload_options by spaces and append each option separately
                for opt in tdload_options.split():
                    tdload_cmd.append(opt)
            elif isinstance(tdload_options, dict):
                # Convert dictionary to command-line options
                for key, value in tdload_options.items():
                    tdload_cmd.append(f"-{key}")
                    if value:
                        tdload_cmd.append(str(value))
            else:
                self.log.warning(
                    "Ignoring tdload_options: expected string or dict, got %s", type(tdload_options)
                )

        # Add the job name as the final argument
        tdload_cmd.append(job_name)

        # Log the command being executed
        self.log.info("Executing tdload command: %s", " ".join(tdload_cmd))

        # Execute the tdload command
        if ssh_conn_id:
            return self._execute_tdload_via_ssh(
                tdload_cmd, tdload_job_variable_file, ssh_conn_id, xcom_push_flag
            )
        return self._execute_tdload_locally(tdload_cmd, source_conn, xcom_push_flag)

    def _execute_tdload_via_ssh(
        self, tdload_cmd, job_variable_file, ssh_conn_id, xcom_push_flag
    ) -> str | None:
        """
        Execute tdload command via SSH connection.

        :param tdload_cmd: The tdload command to execute
        :param job_variable_file: Path to the job variable file
        :param ssh_conn_id: SSH connection ID
        :param xcom_push_flag: Whether to push the result to XCom
        :return: Command result if xcom_push_flag is True, otherwise None
        """
        remote_job_file = f"/tmp/airflow_tdload_{os.path.basename(job_variable_file)}"
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        try:
            with ssh_hook.get_conn() as ssh_client:
                # Copy the job variable file to the remote server
                self.log.info("Copying job variable file to remote server via SFTP: %s", job_variable_file)
                sftp_client = ssh_client.open_sftp()
                try:
                    sftp_client.put(job_variable_file, remote_job_file)
                finally:
                    sftp_client.close()
                self.log.info("Job variable file copied to remote server at %s", remote_job_file)

                # Update command to use remote file path
                tdload_cmd[2] = remote_job_file
                tdload_cmd_str = " ".join(tdload_cmd)

                self.log.info("Executing tdload remotely via SSH on %s: %s", ssh_conn_id, tdload_cmd_str)
                stdin, stdout, stderr = ssh_client.exec_command(tdload_cmd_str)
                output = stdout.read().decode("utf-8")
                error = stderr.read().decode("utf-8")
                self.log.info("tdload command stdout:\n%s", output)
                if error:
                    self.log.error("tdload command stderr:\n%s", error)
                exit_status = stdout.channel.recv_exit_status()
                self.log.info("tdload command exited with status %s", exit_status)

                if exit_status != 0:
                    raise AirflowException(f"tdload command failed with exit code {exit_status}: {error}")

                if xcom_push_flag and output:
                    return output.strip().splitlines()[-1] if output.strip() else "0"
                return None

        except Exception as e:
            self.log.error("Error executing tdload command via SSH: %s", str(e))
            raise AirflowException(f"Error executing tdload command via SSH: {str(e)}")
        finally:
            try:
                with ssh_hook.get_conn() as ssh_client:
                    sftp_client = ssh_client.open_sftp()
                    try:
                        sftp_client.remove(remote_job_file)
                        self.log.info("Remote job variable file cleaned up: %s", remote_job_file)
                    finally:
                        sftp_client.close()
            except Exception as cleanup_error:
                self.log.warning("Failed to clean up remote file %s: %s", remote_job_file, str(cleanup_error))

    def _execute_tdload_locally(self, tdload_cmd, conn, xcom_push_flag) -> str | None:
        """
        Execute tdload command locally.

        :param tdload_cmd: The tdload command to execute
        :param conn: Teradata connection dictionary
        :param xcom_push_flag: Whether to push the result to XCom
        :return: Command result if xcom_push_flag is True, otherwise None
        """
        sp = None
        try:
            self.log.info("Starting local tdload execution")
            sp = subprocess.Popen(
                tdload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid
            )
            conn["sp"] = sp

            # Process output
            last_line = ""
            error_lines = []
            if sp.stdout is not None:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode(conn["console_output_encoding"]).strip()
                    self.log.info(decoded_line)
                    last_line = decoded_line
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            sp.wait()
            self.log.info("tdload command exited with return code %s", sp.returncode)

            if sp.returncode != 0:
                error_msg = "\n".join(error_lines) if error_lines else "Unknown error"
                raise AirflowException(f"tdload command failed with return code {sp.returncode}: {error_msg}")

            if xcom_push_flag:
                return last_line
            return None

        except subprocess.SubprocessError as e:
            self.log.error("Subprocess error during tdload execution: %s", str(e))
            raise AirflowException(f"Subprocess error during tdload execution: {str(e)}")
        except Exception as e:
            self.log.error("Error executing tdload command: %s", str(e))
            raise AirflowException(f"Error executing tdload command: {str(e)}")
        finally:
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
        tdload_options: str | dict | None,
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
        :param tdload_options: Additional tdload options
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

            if source_format.lower() != "delimited":
                job_vars["SourceFormat"] = source_format
            if source_text_delimiter != ",":
                job_vars["SourceTextDelimiter"] = source_text_delimiter

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

            if target_format.lower() != "delimited":
                job_vars["TargetFormat"] = target_format
            if target_text_delimiter != ",":
                job_vars["TargetTextDelimiter"] = target_text_delimiter

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

        if staging_table:
            job_vars["StagingTable"] = staging_table

        # Add any custom options from tdload_options dictionary
        if isinstance(tdload_options, dict):
            for key, value in tdload_options.items():
                if key not in job_vars and value is not None:
                    job_vars[key] = value

        # Format job variables content
        job_var_content = "".join([f"{key}='{value}',\n" for key, value in job_vars.items()])

        # Create a named temporary file that won't be automatically deleted
        with NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as var_file:
            var_file.write(job_var_content)
            var_file_path = var_file.name

        self.log.info("Created job variable file at %s with content:\n%s", var_file_path, job_var_content)

        return var_file_path
