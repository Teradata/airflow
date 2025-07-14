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
"""
Tests for Teradata Parallel Transporter (TPT) operators.

These tests validate the functionality of:
- TdLoadOperator: For data transfers between files and tables
- DdlOperator: For DDL operations on Teradata databases
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.teradata.hooks.tpt import TptHook
from airflow.providers.teradata.operators.tpt import DdlOperator, TdLoadOperator
from airflow.utils.session import provide_session


@pytest.fixture(autouse=True)
def patch_secure_delete():
    """Patch secure_delete for all tests to avoid ValueError from subprocess.run"""
    # Patch the reference used in hooks.tpt, not just tpt_util
    with patch("airflow.providers.teradata.hooks.tpt.secure_delete", return_value=None):
        yield


@pytest.fixture(autouse=True)
def patch_binary_checks():
    # Mock binary availability checks to prevent "binary not found" errors
    with patch("airflow.providers.teradata.hooks.tpt.shutil.which") as mock_which:
        mock_which.return_value = "/usr/bin/mock_binary"  # Always return a path
        yield


@pytest.fixture(autouse=True)
def patch_subprocess():
    # Mock subprocess calls to prevent actual binary execution
    with patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [b"Mock output\n", b""]
        mock_process.communicate.return_value = (b"Mock stdout", b"")
        mock_popen.return_value = mock_process
        yield


class TestTdLoadOperator:
    """
    Tests for TdLoadOperator.

    This test suite validates the TdLoadOperator functionality across different modes:
    - file_to_table: Loading data from a file to a Teradata table
    - table_to_file: Exporting data from a Teradata table to a file
    - select_stmt_to_file: Exporting data from a SQL SELECT statement to a file
    - table_to_table: Transferring data between two Teradata tables

    It also tests parameter validation, error handling, templating, and resource cleanup.
    """

    @pytest.fixture(autouse=True)
    def mock_ssh_verification(self):
        """Mock SSH host key verification to avoid connection issues"""
        with (
            patch("paramiko.client.SSHClient.connect", return_value=None) as mock_connect,
            patch("paramiko.client.SSHClient.get_transport") as mock_transport,
            patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn") as mock_ssh_get_conn,
        ):
            # Mock the transport to avoid AttributeError on set_keepalive
            mock_transport_obj = MagicMock()
            mock_transport.return_value = mock_transport_obj

            # Mock SSH connection
            mock_ssh_client = MagicMock()
            mock_ssh_get_conn.return_value = mock_ssh_client
            yield mock_connect

    @pytest.fixture(autouse=True)
    def mock_verify_tpt_utility(self):
        """Mock the verification of TPT utility on remote host"""
        with patch(
            "airflow.providers.teradata.utils.tpt_util.verify_tpt_utility_on_remote_host", return_value=True
        ) as mock_verify:
            yield mock_verify

    @pytest.fixture(autouse=True)
    def mock_which(self):
        """Mock shutil.which to always find required binaries"""
        with patch("shutil.which", return_value="/usr/bin/tdload") as mock_which:
            yield mock_which

    @pytest.fixture(autouse=True)
    def mock_subprocess(self):
        """Mock subprocess to avoid actual command execution"""
        with patch("subprocess.Popen") as mock_popen:
            # Configure mock process
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.stdout.readline.side_effect = [
                b"Processing data...\n",
                b"1000 rows processed\n",
                b"",
            ]
            mock_popen.return_value = mock_process
            yield mock_popen

    @provide_session
    def setup_method(self, method, session=None):
        """Set up test environment with database connections and DAG"""
        # Clean up any existing test connections
        session.query(Connection).filter(
            Connection.conn_id.in_(["teradata_default", "teradata_target", "teradata_ssl", "ssh_default"])
        ).delete(synchronize_session="fetch")

        # Create source connection
        source_conn = Connection(
            conn_id="teradata_default",
            conn_type="teradata",
            host="source_host",
            login="source_user",
            password="source_password",
        )

        # Create target connection
        target_conn = Connection(
            conn_id="teradata_target",
            conn_type="teradata",
            host="target_host",
            login="target_user",
            password="target_password",
        )

        # Create SSL-enabled connection for SSL tests
        ssl_conn = Connection(
            conn_id="teradata_ssl",
            conn_type="teradata",
            host="ssl_host",
            login="ssl_user",
            password="ssl_password",
            extra='{"use_ssl": true, "ca_cert": "/path/to/ca.pem"}',
        )

        # Create SSH connection for SSH tests
        ssh_conn = Connection(
            conn_id="ssh_default",
            conn_type="ssh",
            host="ssh_host",
            login="ssh_user",
            password="ssh_password",
            port=22,
        )

        # Add all connections to session
        session.add(source_conn)
        session.add(target_conn)
        session.add(ssl_conn)
        session.add(ssh_conn)
        session.commit()

        # Set up a test DAG for templating tests
        self.dag = DAG(
            "test_dag",
            start_date=datetime(2021, 1, 1),
            schedule=None,
            catchup=False,
        )

    # ----- Tests for Basic Operation Modes -----

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_file_to_table_mode(self, mock_execute_tdload):
        """Test loading data from a file to a Teradata table"""
        # Configure mock
        mock_execute_tdload.return_value = 0

        # Create operator
        operator = TdLoadOperator(
            task_id="test_file_to_table",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute operator
        result = operator.execute({})

        # Assertions
        assert result == 0
        mock_execute_tdload.assert_called_once()

        # Verify that the operator initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    def test_file_to_table_with_default_target_conn(self):
        """Test file to table loading with default target connection"""
        # Configure the operator with no target_teradata_conn_id
        operator = TdLoadOperator(
            task_id="test_file_to_table_default_target",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            # No target_teradata_conn_id - should default to teradata_conn_id
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that target_teradata_conn_id was set to teradata_conn_id
        assert operator.target_teradata_conn_id == "teradata_default"
        # Verify that hooks were initialized
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    def test_table_to_file_mode(self):
        """Test exporting data from a Teradata table to a file"""
        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_table_to_file",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hooks were initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is None  # No destination hook for table_to_file

    def test_select_stmt_to_file_mode(self):
        """Test exporting data from a SELECT statement to a file"""
        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_select_to_file",
            select_stmt="SELECT * FROM source_db.source_table WHERE id > 1000",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hooks were initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is None  # No destination hook for select_to_file

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_table_to_table_mode(self, mock_ssh_hook, mock_tpt_hook):
        """Test transferring data between two Teradata tables"""
        # Set up mock objects - configure the mock to return different instances for different calls
        mock_instances = [MagicMock(), MagicMock()]
        mock_tpt_hook.side_effect = mock_instances

        # Configure source hook
        mock_instances[0].execute_tdload.return_value = 0
        mock_instances[0].get_conn.return_value = {
            "host": "src_host",
            "login": "src_user",
            "password": "src_password",
        }

        # Configure destination hook
        mock_instances[1].get_conn.return_value = {
            "host": "dest_host",
            "login": "dest_user",
            "password": "dest_password",
        }

        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_table_to_table",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    # ----- Tests for Advanced Operation Modes -----

    def test_file_to_table_with_insert_stmt(self):
        """Test loading from file to table with custom INSERT statement"""
        # Configure the operator with custom INSERT statement
        operator = TdLoadOperator(
            task_id="test_file_to_table_with_insert",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            insert_stmt="INSERT INTO target_db.target_table (col1, col2) VALUES (?, ?)",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized for file_to_table mode
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    def test_table_to_table_with_select_and_insert(self):
        """Test transferring data between tables with custom SELECT and INSERT statements"""
        # Configure the operator with custom SELECT and INSERT statements
        operator = TdLoadOperator(
            task_id="test_table_to_table_with_select_insert",
            select_stmt="SELECT col1, col2 FROM source_db.source_table WHERE col3 > 1000",
            target_table="target_db.target_table",
            insert_stmt="INSERT INTO target_db.target_table (col1, col2) VALUES (?, ?)",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized for table_to_table mode
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    # ----- Parameter Validation Tests -----

    def test_invalid_parameter_combinations(self):
        """Test validation of invalid parameter combinations"""
        # Test 1: Missing both source and target parameters
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_invalid_params",
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "Invalid parameter combination" in str(context.value)

        # Test 2: Missing target_teradata_conn_id for table_to_table mode
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_missing_target_conn",
                source_table="source_db.source_table",
                target_table="target_db.target_table",
                teradata_conn_id="teradata_default",
                # Missing target_teradata_conn_id for table_to_table
            ).execute({})
        assert "target_teradata_conn_id must be provided" in str(context.value)

        # Test 3: Both source_table and select_stmt provided (contradictory sources)
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_both_source_and_select",
                source_table="source_db.table",
                select_stmt="SELECT * FROM other_db.table",
                target_file_name="/path/to/export.csv",
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "Both source_table and select_stmt cannot be provided simultaneously" in str(context.value)

        # Test 4: insert_stmt without target_table
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_insert_stmt_no_target",
                source_file_name="/path/to/source.csv",
                insert_stmt="INSERT INTO mytable VALUES (?, ?)",
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "insert_stmt is provided but target_table is not specified" in str(context.value)

        # Test 5: Only target_file_name provided (no source)
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_no_source_with_target_file",
                target_file_name="/path/to/file.csv",
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "Invalid parameter combination" in str(context.value)

        # Test 6: Only source_file_name provided (no target)
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_source_file_no_target_table",
                source_file_name="/path/to/source.csv",
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "Invalid parameter combination" in str(context.value)

    # ----- Error Handling Tests -----

    def test_error_handling_execute_tdload(self):
        """Test error handling with invalid connection ID"""
        # Configure the operator with invalid connection ID
        operator = TdLoadOperator(
            task_id="test_error_handling",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="nonexistent_connection",  # This will cause an error
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator and check for exception
        with pytest.raises((AirflowException, ValueError, KeyError)):
            operator.execute({})

    def test_error_handling_get_conn(self):
        """Test error handling with invalid target connection ID"""
        # Configure the operator with invalid target connection ID
        operator = TdLoadOperator(
            task_id="test_error_handling_conn",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="nonexistent_target_connection",  # This will cause an error
        )

        # Execute the operator and check for exception
        with pytest.raises((AirflowException, ValueError, KeyError)):
            operator.execute({})

    # ----- Resource Cleanup Tests -----

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_on_kill(self, mock_ssh_hook, mock_tpt_hook):
        """Test on_kill method cleans up resources properly"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Set up hooks manually
        operator._src_hook = MagicMock()
        operator._dest_hook = MagicMock()

        # Call on_kill
        operator.on_kill()

        # Verify hooks were cleaned up
        operator._src_hook.on_kill.assert_called_once()
        operator._dest_hook.on_kill.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_on_kill_no_hooks(self, mock_tpt_hook):
        """Test on_kill method when no hooks are initialized"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill_no_hooks",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Set hooks to None
        operator._src_hook = None
        operator._dest_hook = None

        # Call on_kill (should not raise any exceptions)
        operator.on_kill()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_on_kill_with_only_src_hook(self, mock_ssh_hook, mock_tpt_hook):
        """Test on_kill with only source hook initialized"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill_src_only",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",  # table_to_file mode
            teradata_conn_id="teradata_default",
        )

        # Set up only source hook
        operator._src_hook = MagicMock()
        operator._dest_hook = None

        # Call on_kill
        operator.on_kill()

        # Verify source hook was cleaned up
        operator._src_hook.on_kill.assert_called_once()

    # ----- Job Variable File Tests -----

    def test_with_local_job_var_file(self):
        """Test using a local job variable file"""
        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True),
            patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content"),
        ):
            # Configure operator with only job var file (no source/target parameters needed)
            operator = TdLoadOperator(
                task_id="test_with_job_var_file",
                tdload_job_var_file="/path/to/job_vars.txt",
                teradata_conn_id="teradata_default",
            )

            # Execute
            result = operator.execute({})

            # Verify the execution was successful (returns 0 for success)
            assert result == 0

    def test_with_local_job_var_file_and_options(self):
        """Test using a local job variable file with additional tdload options"""
        # Set up mocks
        with patch("airflow.providers.teradata.hooks.tpt.TptHook") as mock_tpt_hook:
            mock_tpt_hook_instance = mock_tpt_hook.return_value
            mock_tpt_hook_instance._execute_tdload_locally.return_value = 0

        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True),
            patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content"),
        ):
            # Configure operator with job var file and additional options
            operator = TdLoadOperator(
                task_id="test_with_job_var_file_and_options",
                tdload_job_var_file="/path/to/job_vars.txt",
                tdload_options="-v -u",  # Add verbose and Unicode options
                tdload_job_name="custom_job_name",
                teradata_conn_id="teradata_default",
            )

            # Execute
            result = operator.execute({})

            # Verify the execution was successful (returns 0 for success)
            assert result == 0

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_with_invalid_local_job_var_file(self, mock_ssh_hook, mock_tpt_hook):
        """Test with invalid local job variable file path"""
        # Set up mocks
        with patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=False):
            # Configure operator
            operator = TdLoadOperator(
                task_id="test_with_invalid_job_var_file",
                tdload_job_var_file="/path/to/nonexistent_file.txt",
                teradata_conn_id="teradata_default",
            )

            # Execute and check for exception
            with pytest.raises(ValueError) as context:
                operator.execute({})
            assert "is invalid or does not exist" in str(context.value)

    def test_with_remote_job_var_file(self):
        """Test using a remote job variable file via SSH"""
        # Mock SSH and SFTP operations to avoid actual connections
        with (
            patch("airflow.providers.ssh.hooks.ssh.SSHHook") as mock_ssh_hook,
            patch("airflow.providers.teradata.operators.tpt.is_valid_remote_job_var_file", return_value=True),
            patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tdload_via_ssh", return_value=0),
        ):
            # Set up SSH mock
            mock_ssh_client = MagicMock()
            mock_ssh_hook.return_value.get_conn.return_value = mock_ssh_client

            # Set up SFTP mock for reading remote file - ensure it returns a string
            mock_sftp = MagicMock()
            mock_ssh_client.open_sftp.return_value = mock_sftp
            mock_remote_file = MagicMock()
            mock_remote_file.read.return_value.decode.return_value = "remote job var content"
            mock_sftp.open.return_value.__enter__.return_value = mock_remote_file

            # Configure operator with remote job var file via SSH
            operator = TdLoadOperator(
                task_id="test_with_remote_job_var_file",
                tdload_job_var_file="/remote/path/to/job_vars.txt",
                teradata_conn_id="teradata_default",
                ssh_conn_id="ssh_default",
            )

            # Execute
            result = operator.execute({})

            # Verify the execution was successful (returns 0 for success)
            assert result == 0

    def test_with_remote_job_var_file_and_options(self):
        """Test using a remote job variable file with additional options"""
        # Mock SSH and SFTP operations to avoid actual connections
        with (
            patch("airflow.providers.ssh.hooks.ssh.SSHHook") as mock_ssh_hook,
            patch("airflow.providers.teradata.operators.tpt.is_valid_remote_job_var_file", return_value=True),
            patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tdload_via_ssh", return_value=0),
        ):
            # Set up SSH mock
            mock_ssh_client = MagicMock()
            mock_ssh_hook.return_value.get_conn.return_value = mock_ssh_client

            # Set up SFTP mock for reading remote file - ensure it returns a string
            mock_sftp = MagicMock()
            mock_ssh_client.open_sftp.return_value = mock_sftp
            mock_remote_file = MagicMock()
            mock_remote_file.read.return_value.decode.return_value = "remote job var content"
            mock_sftp.open.return_value.__enter__.return_value = mock_remote_file

            # Configure operator with custom options
            operator = TdLoadOperator(
                task_id="test_with_remote_job_var_file_options",
                tdload_job_var_file="/remote/path/to/job_vars.txt",
                tdload_options="-v -u",
                tdload_job_name="custom_job_name",
                remote_working_dir="/custom/remote/dir",
                teradata_conn_id="teradata_default",
                ssh_conn_id="ssh_default",
            )

            # Execute
            result = operator.execute({})

            # Verify the execution was successful (returns 0 for success)
            assert result == 0

    def test_invalid_remote_job_var_file(self):
        """Test with invalid remote job variable file path"""
        # Set up mocks for SSH connections that should not be called due to validation failure
        with patch(
            "airflow.providers.teradata.operators.tpt.is_valid_remote_job_var_file", return_value=False
        ):
            # Configure operator
            operator = TdLoadOperator(
                task_id="test_with_invalid_remote_job_var_file",
                tdload_job_var_file="/remote/path/to/nonexistent_file.txt",
                teradata_conn_id="teradata_default",
                ssh_conn_id="ssh_default",
            )

            # Execute and check for exception
            with pytest.raises(ValueError) as context:
                operator.execute({})
            assert "invalid or does not exist on remote machine" in str(context.value)

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_remote_job_var_file_missing_ssh_conn(self, mock_ssh_hook, mock_tpt_hook):
        """Test using a remote job var file path without ssh_conn_id"""
        # Configure operator with remote-style path but no ssh_conn_id
        operator = TdLoadOperator(
            task_id="test_with_remote_path_no_ssh",
            tdload_job_var_file="/remote/path/to/job_vars.txt",
            teradata_conn_id="teradata_default",
            # No ssh_conn_id provided
        )

        # Should fall back to local file handling and raise error if file doesn't exist
        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=False),
            pytest.raises(ValueError) as context,
        ):
            operator.execute({})
        assert "is invalid or does not exist" in str(context.value)

    # ----- Template Rendering Tests -----

    def test_template_fields(self):
        """Test templated fields are properly rendered"""
        # Set up operator with templated fields
        operator = TdLoadOperator(
            task_id="test_template_fields",
            source_table="source_{{ ds_nodash }}",
            target_table="target_{{ ds_nodash }}",
            source_file_name="/path/to/{{ ds }}.csv",
            target_file_name="/path/to/export_{{ ds_nodash }}.csv",
            select_stmt="SELECT * FROM {{ ds_nodash }}_table",
            insert_stmt="INSERT INTO {{ ds_nodash }}_table VALUES (?)",
            tdload_options="options_{{ ds_nodash }}",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            dag=self.dag,
        )

        # Set up TaskInstance and DagRun
        execution_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
        run_id = "test_run"
        ti = TaskInstance(task=operator, run_id=run_id)
        ti.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )

        # Render templates
        ti.render_templates()

        # Verify rendered values
        assert operator.source_table == "source_20210101"
        assert operator.target_table == "target_20210101"
        assert operator.source_file_name == "/path/to/2021-01-01.csv"
        assert operator.target_file_name == "/path/to/export_20210101.csv"
        assert operator.select_stmt == "SELECT * FROM 20210101_table"
        assert operator.insert_stmt == "INSERT INTO 20210101_table VALUES (?)"
        assert operator.tdload_options == "options_20210101"

    def test_template_fields_verification(self):
        """Verify that all templated fields are correctly defined"""
        # The operator defines these fields as templated
        expected_template_fields = {
            "source_table",
            "target_table",
            "select_stmt",
            "insert_stmt",
            "source_file_name",
            "target_file_name",
            "tdload_options",
        }

        # Verify that all expected fields are in the actual template_fields
        for field in expected_template_fields:
            assert field in TdLoadOperator.template_fields, f"Field '{field}' should be templated"

        # Verify that template_fields doesn't contain unexpected fields
        assert len(TdLoadOperator.template_fields) == len(expected_template_fields), (
            "TdLoadOperator.template_fields contains unexpected fields"
        )

    # ----- Job Variable Generation Tests -----

    @patch("airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file")
    def test_generated_job_var_content_table_to_table(self, mock_prepare_job_var):
        """Test job variable file is properly generated for table-to-table transfer"""
        # Set up mocks
        mock_prepare_job_var.return_value = "generated job var content"

        # Configure operator for table-to-table mode
        operator = TdLoadOperator(
            task_id="test_generated_job_vars_table_to_table",
            source_table="source_db.table",
            target_table="target_db.table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute
        operator.execute({})

        # Verify prepare_tdload_job_var_file was called (don't check exact connection format)
        mock_prepare_job_var.assert_called_once()
        call_args = mock_prepare_job_var.call_args

        # Verify the important parameters
        assert call_args.kwargs["mode"] == "table_to_table"
        assert call_args.kwargs["source_table"] == "source_db.table"
        assert call_args.kwargs["target_table"] == "target_db.table"
        assert call_args.kwargs["source_format"] == "Delimited"
        assert call_args.kwargs["target_format"] == "Delimited"
        assert call_args.kwargs["source_text_delimiter"] == ","
        assert call_args.kwargs["target_text_delimiter"] == ","

        # Verify connection objects contain expected keys
        assert "host" in call_args.kwargs["source_conn"]
        assert "login" in call_args.kwargs["source_conn"]
        assert "password" in call_args.kwargs["source_conn"]
        assert "host" in call_args.kwargs["target_conn"]
        assert "login" in call_args.kwargs["target_conn"]
        assert "password" in call_args.kwargs["target_conn"]

    @patch("airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file")
    def test_generated_job_var_content_file_to_table(self, mock_prepare_job_var):
        """Test job variable file is properly generated for file-to-table load"""
        # Set up mocks
        mock_prepare_job_var.return_value = "generated job var content"

        # Configure operator for file-to-table mode
        operator = TdLoadOperator(
            task_id="test_generated_job_vars_file_to_table",
            source_file_name="/path/to/data.csv",
            target_table="target_db.table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute
        operator.execute({})

        # Verify prepare_tdload_job_var_file was called
        mock_prepare_job_var.assert_called_once()
        call_args = mock_prepare_job_var.call_args

        # Verify the important parameters
        assert call_args.kwargs["mode"] == "file_to_table"
        assert call_args.kwargs["source_file_name"] == "/path/to/data.csv"
        assert call_args.kwargs["target_table"] == "target_db.table"
        assert call_args.kwargs["source_format"] == "Delimited"
        assert call_args.kwargs["target_format"] == "Delimited"

    @patch("airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file")
    def test_generated_job_var_content_table_to_file(self, mock_prepare_job_var):
        """Test job variable file is properly generated for table-to-file export"""
        # Set up mocks
        mock_prepare_job_var.return_value = "generated job var content"

        # Configure operator for table-to-file mode
        operator = TdLoadOperator(
            task_id="test_generated_job_vars_table_to_file",
            source_table="source_db.table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Execute
        operator.execute({})

        # Verify prepare_tdload_job_var_file was called
        mock_prepare_job_var.assert_called_once()
        call_args = mock_prepare_job_var.call_args

        # Verify the important parameters
        assert call_args.kwargs["mode"] == "table_to_file"
        assert call_args.kwargs["source_table"] == "source_db.table"
        assert call_args.kwargs["target_file_name"] == "/path/to/export.csv"
        assert call_args.kwargs["target_conn"] is None  # No target connection for table-to-file

    @patch("airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file")
    def test_generated_job_var_content_custom_formats(self, mock_prepare_job_var):
        """Test job variable file generation with custom format options"""
        # Set up mocks
        mock_prepare_job_var.return_value = "generated job var content"

        # Configure operator with custom format options
        operator = TdLoadOperator(
            task_id="test_custom_format_options",
            source_table="source_db.table",
            target_table="target_db.target_table",
            source_format="FixedWidth",  # Custom source format
            target_format="Vartext",  # Custom target format
            source_text_delimiter="|",  # Custom source delimiter
            target_text_delimiter="\t",  # Custom target delimiter
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute
        operator.execute({})

        # Verify custom formats were passed to prepare_tdload_job_var_file
        mock_prepare_job_var.assert_called_once()
        call_args = mock_prepare_job_var.call_args

        # Verify the custom format parameters
        assert call_args.kwargs["mode"] == "table_to_table"
        assert call_args.kwargs["source_format"] == "FixedWidth"
        assert call_args.kwargs["target_format"] == "Vartext"
        assert call_args.kwargs["source_text_delimiter"] == "|"
        assert call_args.kwargs["target_text_delimiter"] == "\t"

    # ----- SSH Connection Tests -----

    @patch("airflow.providers.teradata.operators.tpt.get_remote_temp_directory", return_value="/tmp/mock_dir")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_file_to_table_with_ssh(self, mock_ssh_hook, mock_execute_tdload, mock_get_temp_dir):
        """Test loading data from a file to a Teradata table via SSH"""
        # Set up mock SSH hook
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_conn.return_value = MagicMock()
        mock_ssh_hook.return_value = mock_ssh_instance

        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_file_to_table_ssh",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            ssh_conn_id="ssh_default",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hooks were initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is not None
        # Verify mocks were called
        mock_get_temp_dir.assert_called_once()
        mock_execute_tdload.assert_called_once()


class TestDdlOperator:
    """
    Tests for DdlOperator.

    This test suite validates the DdlOperator functionality for:
    - Executing DDL statements on Teradata databases
    - Parameter validation
    - Error handling and error code management
    - Template rendering
    - Resource cleanup
    """

    @provide_session
    def setup_method(self, method, session=None):
        """Set up test environment with database connections"""
        # Clean up any existing test connections
        session.query(Connection).filter(Connection.conn_id.in_(["teradata_default", "teradata_ddl"])).delete(
            synchronize_session="fetch"
        )

        # Create standard connection
        conn = Connection(
            conn_id="teradata_default",
            conn_type="teradata",
            host="test_host",
            login="test_user",
            password="test_password",
        )

        # Create dedicated DDL connection with specific options
        ddl_conn = Connection(
            conn_id="teradata_ddl",
            conn_type="teradata",
            host="ddl_host",
            login="ddl_user",
            password="ddl_password",
            extra='{"session_params": {"QUERY_BAND": "DDL_OPERATION=TRUE;"}}',
        )

        # Add connections to session
        session.add(conn)
        session.add(ddl_conn)
        session.commit()

        # Set up a test DAG for templating tests
        self.dag = DAG(
            "test_ddl_dag",
            start_date=datetime(2021, 1, 1),
            schedule=None,
            catchup=False,
        )

    # ----- DDL Execution Tests -----

    def test_ddl_execution(self):
        """Test executing DDL statements"""
        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl",
            ddl=["CREATE TABLE test_db.test_table (id INT)", "CREATE INDEX idx ON test_db.test_table (id)"],
            teradata_conn_id="teradata_default",
            ddl_job_name="test_ddl_job",
        )

        # Execute
        result = operator.execute({})

        # Verify the execution was successful (returns 0 for success)
        assert result == 0

    def test_ddl_execution_with_multiple_statements(self):
        """Test executing multiple DDL statements"""
        # Create a list of complex DDL statements
        ddl_statements = [
            "CREATE TABLE test_db.customers (customer_id INTEGER, name VARCHAR(100), email VARCHAR(255))",
            "CREATE INDEX idx_customer_name ON test_db.customers (name)",
            """CREATE TABLE test_db.orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                FOREIGN KEY (customer_id) REFERENCES test_db.customers(customer_id)
            )""",
            "CREATE PROCEDURE test_db.get_customer(IN p_id INTEGER) BEGIN SELECT * FROM test_db.customers WHERE customer_id = p_id; END;",
        ]

        # Configure operator with multiple statements
        operator = DdlOperator(
            task_id="test_multiple_ddl",
            ddl=ddl_statements,
            teradata_conn_id="teradata_default",
        )

        # Execute
        result = operator.execute({})

        # Verify the execution was successful (returns 0 for success)
        assert result == 0

    # ----- Parameter Validation Tests -----

    def test_ddl_parameter_validation(self):
        """Test validation of DDL parameter"""
        # Test empty DDL list
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_empty_ddl",
                ddl=[],
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "ddl parameter must be a non-empty list" in str(context.value)

        # Test non-list DDL parameter
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_non_list_ddl",
                ddl="CREATE TABLE test_table (id INT)",  # string instead of list
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "ddl parameter must be a non-empty list" in str(context.value)

        # Test DDL with empty string
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_empty_string_ddl",
                ddl=["CREATE TABLE test_table (id INT)", ""],
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "ddl parameter must be a non-empty list" in str(context.value)

        # Test DDL with None value
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_none_ddl",
                ddl=None,
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "ddl parameter must be a non-empty list" in str(context.value)

        # Test DDL with list containing non-string values
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_non_string_ddl",
                ddl=["CREATE TABLE test_table (id INT)", 123],
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "ddl parameter must be a non-empty list" in str(context.value)

    def test_error_list_validation(self):
        """Test validation of error_list parameter"""
        # Test with integer error code
        operator = DdlOperator(
            task_id="test_int_error_list",
            ddl=["CREATE TABLE test_table (id INT)"],
            error_list=3803,  # single integer
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0
        assert operator.error_list == 3803  # Original value should remain unchanged

        # Test with list of integers
        operator = DdlOperator(
            task_id="test_list_error_list",
            ddl=["CREATE TABLE test_table (id INT)"],
            error_list=[3803, 3807, 5495],  # list of integers
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0
        assert operator.error_list == [3803, 3807, 5495]

        # Test with invalid error_list type (string)
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_invalid_error_list_string",
                ddl=["CREATE TABLE test_table (id INT)"],
                error_list="3803",  # string instead of int or list
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "error_list must be an int or a list of ints" in str(context.value)

        # Test with invalid error_list type (dict)
        with pytest.raises(ValueError) as context:
            DdlOperator(
                task_id="test_invalid_error_list_dict",
                ddl=["CREATE TABLE test_table (id INT)"],
                error_list={"code": 3803},  # dict instead of int or list
                teradata_conn_id="teradata_default",
            ).execute({})
        assert "error_list must be an int or a list of ints" in str(context.value)

        # NOTE: The current operator implementation does not validate the contents of the error_list
        # so lists with mixed types will not raise an error. This could be improved in the future.

    # ----- Error Handling Tests -----

    def test_ddl_execution_with_error_handling(self):
        """Test DDL execution with error handling"""
        # Configure operator with error list
        operator = DdlOperator(
            task_id="test_ddl_with_errors",
            ddl=[
                "DROP TABLE test_db.nonexistent_table",  # This might generate error 3807 (object not found)
                "CREATE TABLE test_db.new_table (id INT)",
            ],
            error_list=[3807],  # Ignore "object does not exist" errors
            teradata_conn_id="teradata_default",
        )

        # Execute
        result = operator.execute({})

        # Verify the execution was successful (returns 0 for success)
        assert result == 0

    def test_ddl_execution_error(self):
        """Test error handling during DDL execution"""
        # This test verifies the normal case since we can't easily simulate real DDL errors
        # In a real environment, DDL errors would be handled by the TPT hooks

        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_execution_error",
            ddl=["CREATE TABLE test_db.test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Execute and verify it doesn't crash
        result = operator.execute({})
        assert result == 0

    # ----- Resource Cleanup Tests -----

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_ddl_on_kill(self, mock_tpt_hook):
        """Test on_kill method for DDL operator"""
        # Set up mocks
        mock_hook_instance = mock_tpt_hook.return_value

        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_on_kill",
            ddl=["CREATE TABLE test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Set hook manually
        operator._hook = mock_hook_instance

        # Call on_kill
        operator.on_kill()

        # Verify hook was cleaned up
        mock_hook_instance.on_kill.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_ddl_on_kill_no_hook(self, mock_tpt_hook):
        """Test on_kill method when no hook is initialized"""
        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_on_kill_no_hook",
            ddl=["CREATE TABLE test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Set hook to None
        operator._hook = None

        # Call on_kill (should not raise any exceptions)
        operator.on_kill()

        # Verify that no exception is raised and a warning is logged
        # Note: We can't directly test the logging, but we can verify the code doesn't fail

    # ----- Template Tests -----

    def test_template_fields(self):
        """Test that template fields are properly rendered"""
        # Configure operator with templated fields
        operator = DdlOperator(
            task_id="test_ddl_template_fields",
            ddl=["CREATE TABLE {{ ds_nodash }}_table (id INT)"],
            ddl_job_name="ddl_job_{{ ds_nodash }}",
            teradata_conn_id="teradata_default",
            dag=self.dag,
        )

        # Set up TaskInstance and DagRun
        execution_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
        run_id = "test_run"
        ti = TaskInstance(task=operator, run_id=run_id)
        ti.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )

        # Render templates
        ti.render_templates()

        # Verify rendered values
        assert operator.ddl == ["CREATE TABLE 20210101_table (id INT)"]
        assert operator.ddl_job_name == "ddl_job_20210101"

    def test_template_fields_multiple_statements(self):
        """Test templating with multiple DDL statements"""
        # Configure operator with multiple templated DDL statements
        operator = DdlOperator(
            task_id="test_multiple_template_fields",
            ddl=[
                "CREATE TABLE {{ ds_nodash }}_customers (customer_id INT)",
                "CREATE TABLE {{ ds_nodash }}_orders (order_id INT)",
                "CREATE INDEX idx_{{ ds_nodash }} ON {{ ds_nodash }}_customers (customer_id)",
            ],
            ddl_job_name="ddl_job_{{ ds_nodash }}",
            teradata_conn_id="teradata_default",
            dag=self.dag,
        )

        # Set up TaskInstance and DagRun
        execution_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
        run_id = "test_run"
        ti = TaskInstance(task=operator, run_id=run_id)
        ti.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )

        # Render templates
        ti.render_templates()

        # Verify rendered values
        assert operator.ddl == [
            "CREATE TABLE 20210101_customers (customer_id INT)",
            "CREATE TABLE 20210101_orders (order_id INT)",
            "CREATE INDEX idx_20210101 ON 20210101_customers (customer_id)",
        ]

    def test_template_fields_verification(self):
        """Verify that all templated fields are correctly defined"""
        # The operator defines these fields as templated
        expected_template_fields = {"ddl", "ddl_job_name"}

        # Verify that all expected fields are in the actual template_fields
        for field in expected_template_fields:
            assert field in DdlOperator.template_fields, f"Field '{field}' should be templated"

        # Verify that template_fields doesn't contain unexpected fields
        assert len(DdlOperator.template_fields) == len(expected_template_fields), (
            "DdlOperator.template_fields contains unexpected fields"
        )

    def test_template_ext(self):
        """Test that the template extensions are correctly defined"""
        # Verify template_ext contains .sql
        assert ".sql" in DdlOperator.template_ext

        # Verify that operator can read from SQL file (simulation)
        # In a real execution, Airflow would read the SQL file and pass the content to the operator
        # Here we simulate that the SQL file has been read and its content provided to the operator
        operator = DdlOperator(
            task_id="test_sql_file",
            ddl=["SELECT * FROM test_table;"],  # This would be the content of the SQL file
            teradata_conn_id="teradata_default",
        )

        # Execute and verify no errors
        result = operator.execute({})
        assert result == 0

    # ----- SSL Connection Tests -----

    def test_ssl_connection(self):
        """Test operations with SSL-enabled Teradata connection"""
        # Configure the operator with SSL connection
        operator = TdLoadOperator(
            task_id="test_ssl_connection",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_ssl",  # SSL-enabled connection
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results - focus on successful execution with SSL
        assert result == 0
        # Verify that hooks were initialized correctly for SSL connections
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    def test_ddl_with_ssl_connection(self):
        """Test executing DDL statements with SSL connection"""
        with patch(
            "airflow.providers.teradata.operators.tpt.prepare_tpt_ddl_script", return_value="SSL DDL script"
        ):
            # Configure operator with SSL connection
            operator = DdlOperator(
                task_id="test_ssl_ddl",
                ddl=["CREATE TABLE ssl_db.ssl_table (id INT)"],
                teradata_conn_id="teradata_ssl",  # SSL-enabled connection
            )

            # Execute
            result = operator.execute({})

            # Verify successful execution with SSL connection
            assert result == 0
            # Verify hook was initialized for SSL connection
            assert operator._hook is not None

    # ----- Custom Working Directory Tests -----

    def test_custom_working_directory(self):
        """Test specifying a custom working directory for temporary files"""
        # Configure operator with custom working directory
        operator = TdLoadOperator(
            task_id="test_custom_working_dir",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
            remote_working_dir="/custom/temp/dir",  # Custom working directory
        )

        # Execute
        result = operator.execute({})

        # Verify successful execution with custom directory
        assert result == 0
        # Verify that the custom working directory parameter is set
        assert operator.remote_working_dir == "/custom/temp/dir"
        # Verify that hook was initialized
        assert operator._src_hook is not None

    @patch("airflow.providers.teradata.operators.tpt.get_remote_temp_directory", return_value="/tmp/mock_dir")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_ddl", return_value=0)
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_ddl_with_ssh(self, mock_ssh_hook, mock_execute_ddl, mock_get_temp_dir):
        """Test executing DDL statements via SSH connection"""
        # Set up mock SSH hook
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_conn.return_value = MagicMock()
        mock_ssh_hook.return_value = mock_ssh_instance

        # Configure operator with SSH connection
        operator = DdlOperator(
            task_id="test_ddl_with_ssh",
            ddl=["CREATE TABLE test_db.test_table (id INT)"],
            teradata_conn_id="teradata_default",
            ssh_conn_id="ssh_default",
            remote_working_dir="/custom/working/dir",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hook was initialized correctly
        assert operator._hook is not None
        # Verify execute_ddl was called
        mock_execute_ddl.assert_called_once()

    # ----- Specific subprocess mocking tests -----

    @patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen")
    @patch("airflow.providers.teradata.hooks.tpt.shutil.which")
    def test_direct_tdload_execution_mocking(self, mock_which, mock_popen):
        """Test the direct execution of tdload with proper mocking"""
        # Ensure the binary is found
        mock_which.return_value = "/usr/bin/tdload"

        # Mock the subprocess
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = MagicMock()
        mock_process.stdout.readline.side_effect = [
            b"Starting TDLOAD...\n",
            b"Processing data...\n",
            b"1000 rows loaded successfully\n",
            b"",
        ]
        mock_popen.return_value = mock_process

        # Create the TPT hook directly
        hook = TptHook(teradata_conn_id="teradata_default")

        # Execute the command directly
        result = hook._execute_tdload_locally(
            job_var_content="DEFINE JOB sample_job;\nUSING OPERATOR sel;\nSELECT * FROM source_table;\n",
            tdload_options="-v",
            tdload_job_name="sample_job",
        )

        # Verify the result
        assert result == 0
        mock_popen.assert_called_once()
        cmd_args = mock_popen.call_args[0][0]
        assert cmd_args[0] == "tdload"
        assert "-j" in cmd_args
        assert "-v" in cmd_args
        assert "sample_job" in cmd_args

    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tdload_locally")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    def test_execute_with_local_job_var_file_direct_patch(
        self, mock_hook_init, mock_execute_local, mock_dest_hook, mock_src_hook
    ):
        """Test TdLoadOperator with a local job var file using direct patching (bteq style)"""
        # Arrange
        mock_execute_local.return_value = 0
        operator = TdLoadOperator(
            task_id="test_with_local_job_var_file_direct_patch",
            tdload_job_var_file="/path/to/job_vars.txt",
            teradata_conn_id="teradata_default",
        )
        # Manually set hooks since we bypassed __init__
        operator._src_hook = mock_src_hook
        operator._dest_hook = mock_dest_hook
        operator._src_hook._execute_tdload_locally = mock_execute_local
        # Patch file validation and reading
        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True),
            patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content"),
        ):
            # Act
            result = operator.execute({})
        # Assert
        mock_execute_local.assert_called_once_with("job var content", None, None)
        assert result == 0

    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    def test_on_kill_direct_patch(self, mock_hook_init, mock_dest_hook, mock_src_hook):
        """Test on_kill method with direct patching (bteq style)"""
        operator = TdLoadOperator(
            task_id="test_on_kill_direct_patch",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )
        # Set up mocked attributes
        operator._src_hook = mock_src_hook
        operator._dest_hook = mock_dest_hook
        # Ensure the mocked hooks have on_kill methods
        mock_src_hook.on_kill = Mock()
        mock_dest_hook.on_kill = Mock()

        # Act
        operator.on_kill()

        # Assert
        mock_src_hook.on_kill.assert_called_once()
        mock_dest_hook.on_kill.assert_called_once()

    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    def test_on_kill_no_hooks_direct_patch(self, mock_hook_init, mock_dest_hook, mock_src_hook):
        """Test on_kill method when no hooks are initialized (bteq style)"""
        operator = TdLoadOperator(
            task_id="test_on_kill_no_hooks_direct_patch",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )
        operator._src_hook = None
        operator._dest_hook = None
        # Act
        operator.on_kill()
