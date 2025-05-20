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
    def setUp(self):
        self.mock_context = {}
        self.task_id = "test_bteq_operator"
        self.bteq = "SELECT * FROM my_table;"
        self.ttu_conn_id = "teradata_default"

    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_with_xcom_push(self, mock_hook_init, mock_execute_bteq):
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
            xcom_push_flag=True,
            ttu_conn_id=self.ttu_conn_id,
        )

        # When
        result = operator.execute(self.mock_context)

        # Then
        mock_hook_init.assert_called_once_with(ttu_conn_id=self.ttu_conn_id)
        mock_execute_bteq.assert_called_once_with(self.bteq, True)
        assert result == expected_result

    @mock.patch.object(BteqHook, "execute_bteq")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_without_xcom_push(self, mock_hook_init, mock_execute_bteq):
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
            xcom_push_flag=False,
            ttu_conn_id=self.ttu_conn_id,
        )

        # When
        result = operator.execute(self.mock_context)

        # Then
        mock_hook_init.assert_called_once_with(ttu_conn_id=self.ttu_conn_id)
        mock_execute_bteq.assert_called_once_with(self.bteq, False)
        assert result is None

    @mock.patch.object(BteqHook, "on_kill")
    def test_on_kill(self, mock_on_kill):
        # Given
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
        )
        operator._hook = BteqHook()

        # When
        operator.on_kill()

        # Then
        mock_on_kill.assert_called_once()

    def test_on_kill_not_initialized(self):
        # Given
        operator = BteqOperator(
            task_id=self.task_id,
            bteq=self.bteq,
        )
        operator._hook = None

        # When/Then (no exception should be raised)
        operator.on_kill()

    def test_template_fields(self):
        # Verify template fields are defined correctly
        assert BteqOperator.template_fields == ("bteq",)

    def test_template_ext(self):
        # Verify template extensions are defined correctly
        assert BteqOperator.template_ext == (".sql", ".bteq")


if __name__ == "__main__":
    unittest.main()
