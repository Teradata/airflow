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
from unittest import mock

from airflow.providers.teradata.hooks.bteq import BteqHook
from airflow.providers.teradata.operators.bteq import BteqOperator


class TestBteqOperator:
    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_with_xcom_push(self, mock_hook_init, mock_execute_bteq):
        task_id = "test_bteq_operator"
        bteq = "SELECT * FROM my_table;"
        teradata_conn_id = "teradata_default"
        mock_context = {}
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=task_id,
            bteq=bteq,
            xcom_push_flag=True,
            teradata_conn_id=teradata_conn_id,
        )

        # When
        result = operator.execute(mock_context)
        # Then
        mock_hook_init.assert_called_once_with(teradata_conn_id=teradata_conn_id, ssh_conn_id=None)
        mock_execute_bteq.assert_called_once_with("SELECT * FROM my_table;", True)
        assert result == expected_result

    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_without_xcom_push(self, mock_hook_init, mock_execute_bteq):
        task_id = "test_bteq_operator"
        bteq = "SELECT * FROM my_table;"
        teradata_conn_id = "teradata_default"
        mock_context = {}
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=task_id,
            bteq=bteq,
            xcom_push_flag=False,
            teradata_conn_id=teradata_conn_id,
        )

        # When
        result = operator.execute(mock_context)

        # Then
        mock_hook_init.assert_called_once_with(teradata_conn_id=teradata_conn_id, ssh_conn_id=None)
        mock_execute_bteq.assert_called_once_with(bteq, False)
        assert result is None

    @mock.patch.object(BteqHook, "on_kill")
    def test_on_kill(self, mock_on_kill):
        task_id = "test_bteq_operator"
        bteq = "SELECT * FROM my_table;"
        # Given
        operator = BteqOperator(
            task_id=task_id,
            bteq=bteq,
        )
        operator._hook = BteqHook()

        # When
        operator.on_kill()

        # Then
        mock_on_kill.assert_called_once()

    def test_on_kill_not_initialized(self):
        task_id = "test_bteq_operator"
        bteq = "SELECT * FROM my_table;"
        # Given
        operator = BteqOperator(
            task_id=task_id,
            bteq=bteq,
        )
        operator._hook = None

        # When/Then (no exception should be raised)
        operator.on_kill()

    def test_template_fields(self):
        # Verify template fields are defined correctly
        print(BteqOperator.template_fields)
        assert BteqOperator.template_fields == "bteq"

    def test_template_ext(self):
        # Verify template extensions are defined correctly
        assert BteqOperator.template_ext == (".sql", ".bteq")


if __name__ == "__main__":
    unittest.main()
