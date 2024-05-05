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

from unittest import mock
from unittest.mock import MagicMock, Mock

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.operators.teradata import TeradataOperator, TeradataStoredProcedureOperator
from airflow.providers.teradata.operators.teradata_compute_cluster import \
    TeradataComputeClusterProvisionOperator


class TestTeradataOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_get_hook_from_conn(self, mock_get_db_hook):
        """
        :class:`~.TeradataOperator` should use the hook returned by :meth:`airflow.models.Connection.get_hook`
        if one is returned.

        Specifically we verify here that :meth:`~.TeradataOperator.get_hook` returns the hook returned from a
        call of ``get_hook`` on the object returned from :meth:`~.BaseHook.get_connection`.
        """
        mock_hook = MagicMock()
        mock_get_db_hook.return_value = mock_hook

        op = TeradataOperator(task_id="test", sql="")
        assert op.get_db_hook() == mock_hook

    @mock.patch(
        "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook",
        autospec=TeradataHook,
    )
    def test_get_hook_default(self, mock_get_db_hook):
        """
        If :meth:`airflow.models.Connection.get_hook` does not return a hook (e.g. because of an invalid
        conn type), then :class:`~.TeradataHook` should be used.
        """
        mock_get_db_hook.return_value.side_effect = Mock(side_effect=AirflowException())

        op = TeradataOperator(task_id="test", sql="")
        assert op.get_db_hook().__class__.__name__ == "TeradataHook"

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute(self, mock_get_db_hook):
        sql = "SELECT * FROM test_table"
        conn_id = "teradata_default"
        parameters = {"parameter": "value"}
        autocommit = False
        context = "test_context"
        task_id = "test_task_id"

        operator = TeradataOperator(sql=sql, conn_id=conn_id, parameters=parameters, task_id=task_id)
        operator.execute(context=context)
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=sql,
            autocommit=autocommit,
            parameters=parameters,
            handler=fetch_all_handler,
            return_last=True,
        )

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_teradata_operator_test_multi(self, mock_get_db_hook):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        conn_id = "teradata_default"
        parameters = {"parameter": "value"}
        autocommit = False
        context = "test_context"
        task_id = "test_task_id"

        operator = TeradataOperator(sql=sql, conn_id=conn_id, parameters=parameters, task_id=task_id)
        operator.execute(context=context)
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=sql,
            autocommit=autocommit,
            parameters=parameters,
            handler=fetch_all_handler,
            return_last=True,
        )


class TestTeradataComputeClusterProvisionOperator:
    @mock.patch("airflow.providers.teradata.transfers.azure_blob_to_teradata.TeradataHook")
    def test_execute_hook(self, mock_hook):
        compute_profile_name = "test"
        computer_group_name = "test"
        query_strategy = "test"
        compute_map = "test"
        compute_attribute = "test"
        conn_id = "teradata_default"
        timeout = 1
        context = "test_context"
        task_id = "test_task_id"

        operator = TeradataComputeClusterProvisionOperator(
            compute_profile_name=compute_profile_name,
            computer_group_name=computer_group_name,
            query_strategy=query_strategy,
            compute_map=compute_map,
            compute_attribute=compute_attribute,
            conn_id=conn_id,
            timeout=timeout,
            task_id=task_id,
        )
        operator.execute(context=context)
        mock_hook.assert_called_once_with(teradata_conn_id=conn_id)

        @mock.patch.object(TeradataComputeClusterProvisionOperator, "compute_cluster_execute",
                           autospec=TeradataComputeClusterProvisionOperator.compute_cluster_execute)
        def test_mock_compute_cluster_execute(self, mock_compute_cluster_execute):
            compute_profile_name = "test"
            computer_group_name = "test"
            query_strategy = "test"
            compute_map = "test"
            compute_attribute = "test"
            conn_id = "teradata_default"
            timeout = 1
            context = "test_context"
            task_id = "test_task_id"

            operator = TeradataComputeClusterProvisionOperator(
                compute_profile_name=compute_profile_name,
                computer_group_name=computer_group_name,
                query_strategy=query_strategy,
                compute_map=compute_map,
                compute_attribute=compute_attribute,
                conn_id=conn_id,
                timeout=timeout,
                task_id=task_id,
            )
            result = operator.execute(context=context)
            assert result is mock_compute_cluster_execute.return_value
            mock_compute_cluster_execute.assert_called_once_with(
                mock.ANY,
                "CREATE",
            )


