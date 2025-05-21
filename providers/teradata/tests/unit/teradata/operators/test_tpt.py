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

import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, Connection, TaskInstance
from airflow.providers.teradata.operators.tpt import TdLoadOperator
from airflow.utils.session import provide_session


# Add this patch at the class level to intercept all subprocess calls
@patch("subprocess.Popen")
class TestTdLoadOperator:
    """Tests for TdLoadOperator"""

    @provide_session
    def setup_method(self, method, session=None):
        # Delete existing connections if they exist
        session.query(Connection).filter(
            Connection.conn_id.in_(["teradata_default", "teradata_target"])
        ).delete(synchronize_session="fetch")

        # Set up mock connections
        source_conn = Connection(
            conn_id="teradata_default",
            conn_type="teradata",
            host="source_host",
            login="source_user",
            password="source_password",
        )
        target_conn = Connection(
            conn_id="teradata_target",
            conn_type="teradata",
            host="target_host",
            login="target_user",
            password="target_password",
        )
        session.add(source_conn)
        session.add(target_conn)
        session.commit()

        # Create a dag for template rendering tests
        self.dag = DAG(
            "test_dag",
            start_date=datetime(2021, 1, 1),
            schedule=None,
            catchup=False,
        )

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_error_handling(self, mock_execute_tdload, mock_popen):
        """Test error handling"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Configure mock to raise an exception
        mock_execute_tdload.side_effect = Exception("Connection failed")

        # Create operator
        operator = TdLoadOperator(
            task_id="test_error_handling",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Assertions
        with pytest.raises(AirflowException) as context:
            operator.execute({})

        assert "File to table loading failed" in str(context.value)

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_file_to_table_mode(self, mock_execute_tdload, mock_popen):
        """Test file to table mode"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Configure mock
        mock_execute_tdload.return_value = "1000 rows loaded successfully"

        # Create operator
        operator = TdLoadOperator(
            task_id="test_file_to_table",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            xcom_push_flag=True,
        )

        # Mock context for xcom_push
        context = {"ti": mock.MagicMock()}

        # Execute operator
        result = operator.execute(context)

        # Assertions
        assert result == "1000 rows loaded successfully"
        assert operator.mode == "file_to_table"
        mock_execute_tdload.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(
            key="tdload_result", value="1000 rows loaded successfully"
        )

    def test_invalid_parameter_combinations(self, mock_popen):
        """Test invalid parameter combinations"""
        # Missing both source and target parameters
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_invalid_params",
                teradata_conn_id="teradata_default",
            )
        assert "Invalid parameter combination" in str(context.value)

        # Missing target_teradata_conn_id for table_to_table mode
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_missing_target_conn",
                source_table="source_db.source_table",
                target_table="target_db.target_table",
                teradata_conn_id="teradata_default",
            )
        assert "target_teradata_conn_id must be provided" in str(context.value)

    def test_on_kill(self, mock_popen):
        """Test the on_kill method"""
        # Create operator
        operator = TdLoadOperator(
            task_id="test_on_kill",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Set the hooks with mocks
        operator._src_hook = MagicMock()
        operator._dest_hook = MagicMock()

        # Call on_kill
        operator.on_kill()

        # Verify on_kill was called on both hooks
        operator._src_hook.on_kill.assert_called_once()
        operator._dest_hook.on_kill.assert_called_once()

    def test_source_and_select_stmt_validation(self, mock_popen):
        """Test that providing both source_table and select_stmt raises ValueError"""
        with pytest.raises(ValueError) as context:
            TdLoadOperator(
                task_id="test_validation",
                source_table="source_db.table",
                select_stmt="SELECT * FROM other_db.table",
                target_file_name="/path/to/export.csv",
                teradata_conn_id="teradata_default",
            )

        assert "Both source_table and select_stmt cannot be provided", str(context.exception)

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_table_to_file_mode(self, mock_execute_tdload, mock_popen):
        """Test table to file mode"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Configure mock
        mock_execute_tdload.return_value = "1000 rows exported successfully"

        # Create operator
        operator = TdLoadOperator(
            task_id="test_table_to_file",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
            xcom_push_flag=True,
        )

        # Mock context for xcom_push
        context = {"ti": mock.MagicMock()}

        # Execute operator
        result = operator.execute(context)

        # Assertions
        assert result == "1000 rows exported successfully"
        assert operator.mode == "table_to_file"
        mock_execute_tdload.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(
            key="tdload_result", value="1000 rows exported successfully"
        )

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_table_to_table_mode(self, mock_execute_tdload, mock_popen):
        """Test table to table mode"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Configure mock
        mock_execute_tdload.return_value = "1000 rows transferred successfully"

        # Create operator
        operator = TdLoadOperator(
            task_id="test_table_to_table",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            xcom_push_flag=True,
        )

        # Mock context for xcom_push
        context = {"ti": mock.MagicMock()}

        # Execute operator
        result = operator.execute(context)

        # Assertions
        assert result == "1000 rows transferred successfully"
        assert operator.mode == "table_to_table"
        mock_execute_tdload.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(
            key="tdload_result", value="1000 rows transferred successfully"
        )

    def test_tdload_on_kill_no_hooks(self, mock_popen):
        """Test on_kill when hooks are None"""
        operator = TdLoadOperator(
            task_id="test_kill_none",
            source_table="source_db.table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Explicitly set hooks to None
        operator._src_hook = None
        operator._dest_hook = None

        # Should not raise an exception
        operator.on_kill()

    def test_tdload_source_hook_on_kill(self, mock_popen):
        """Test that on_kill calls on_kill on the source hook only"""
        # Create operator
        operator = TdLoadOperator(
            task_id="test_kill_src",
            source_table="source_db.table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Create mock hook
        operator._src_hook = MagicMock()
        operator._dest_hook = None

        # Call on_kill
        operator.on_kill()

        # Verify on_kill was called on source hook
        operator._src_hook.on_kill.assert_called_once()

    def test_templated_fields(self, mock_popen):
        """Test that templated fields work correctly"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Create templated values
        source_table = "source_{{ ds_nodash }}"
        target_table = "target_{{ ds_nodash }}"
        source_file = "/path/to/{{ ds }}.csv"
        target_file = "/path/to/export_{{ ds_nodash }}.csv"

        # Test file to table with templating
        f2t_operator = TdLoadOperator(
            task_id="test_f2t_templating",
            source_file_name=source_file,
            target_table=target_table,
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            dag=self.dag,
        )

        # Test table to file with templating
        t2f_operator = TdLoadOperator(
            task_id="test_t2f_templating",
            source_table=source_table,
            target_file_name=target_file,
            teradata_conn_id="teradata_default",
            dag=self.dag,
        )

        # Test table to table with templating
        t2t_operator = TdLoadOperator(
            task_id="test_t2t_templating",
            source_table=source_table,
            target_table=target_table,
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            dag=self.dag,
        )

        # Import DagRun model
        # Create proper DagRun objects with timezone-aware datetime
        from datetime import timezone

        from airflow.models import DagRun

        execution_date = datetime(2021, 1, 1, tzinfo=timezone.utc)

        # Render templates for all operators
        run_id_f2t = "test_f2t_run"
        ti_f2t = TaskInstance(task=f2t_operator, run_id=run_id_f2t)
        ti_f2t.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id_f2t,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )
        ti_f2t.render_templates()

        run_id_t2f = "test_t2f_run"
        ti_t2f = TaskInstance(task=t2f_operator, run_id=run_id_t2f)
        ti_t2f.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id_t2f,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )
        ti_t2f.render_templates()

        run_id_t2t = "test_t2t_run"
        ti_t2t = TaskInstance(task=t2t_operator, run_id=run_id_t2t)
        ti_t2t.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id=run_id_t2t,
            logical_date=execution_date,
            start_date=execution_date,
            run_after=execution_date,
            run_type="manual",
        )
        ti_t2t.render_templates()

        # Check rendered values
        assert f2t_operator.source_file_name == "/path/to/2021-01-01.csv"
        assert f2t_operator.target_table == "target_20210101"

        assert t2f_operator.source_table == "source_20210101"
        assert t2f_operator.target_file_name == "/path/to/export_20210101.csv"

        assert t2t_operator.source_table == "source_20210101"
        assert t2t_operator.target_table == "target_20210101"

    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload")
    def test_xcom_push_disabled(self, mock_execute_tdload, mock_popen):
        """Test that xcom_push works correctly when disabled"""
        # Setup mock popen for subprocess calls
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"success output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Configure mock
        mock_execute_tdload.return_value = "1000 rows loaded successfully"

        # Create operator with xcom_push disabled
        operator = TdLoadOperator(
            task_id="test_xcom_disabled",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
            xcom_push_flag=False,
        )

        # Mock context for xcom_push
        context = {"ti": mock.MagicMock()}

        # Execute operator
        result = operator.execute(context)

        # Assertions
        assert result == "1000 rows loaded successfully"
        context["ti"].xcom_push.assert_not_called()


if __name__ == "__main__":
    unittest.main()
