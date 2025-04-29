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
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.providers.teradata.operators.bteq import BteqOperator


class TestBteqOperator(unittest.TestCase):
    def setUp(self):
        self.bteq_operator = BteqOperator(
            task_id="test_bteq_operator",
            bteq="SELECT * FROM test_table;",
            ttu_conn_id="teradata_default",
            xcom_push=True,
        )

    def test_init(self):
        self.assertEqual(self.bteq_operator.bteq, "SELECT * FROM test_table;")
        self.assertEqual(self.bteq_operator.ttu_conn_id, "teradata_default")
        self.assertEqual(self.bteq_operator.xcom_push, True)

    @patch("airflow.providers.teradata.operators.bteq.TtuHook")
    def test_execute(self, mock_ttu_hook):
        mock_hook_instance = MagicMock()
        mock_ttu_hook.return_value = mock_hook_instance
        context = {}
        self.bteq_operator.execute(context)
        mock_ttu_hook.assert_called_once_with(ttu_conn_id="teradata_default")
        mock_hook_instance.execute_bteq.assert_called_once_with("SELECT * FROM test_table;", True)

    @patch("airflow.providers.teradata.operators.bteq.TtuHook")
    def test_on_kill(self, mock_ttu_hook):
        mock_hook_instance = MagicMock()
        mock_ttu_hook.return_value = mock_hook_instance
        self.bteq_operator._hook = mock_hook_instance
        self.bteq_operator.on_kill()
        mock_hook_instance.on_kill.assert_called_once()

    @patch("airflow.providers.teradata.operators.bteq.TtuHook")
    def test_on_kill_hook_not_initialized(self, mock_ttu_hook):
        mock_hook_instance = MagicMock()
        mock_ttu_hook.return_value = mock_hook_instance
        self.bteq_operator._hook = None
        self.bteq_operator.on_kill()
        mock_hook_instance.on_kill.assert_not_called()

    @patch("airflow.providers.teradata.operators.bteq.TtuHook")
    def test_execute_exception(self, mock_ttu_hook):
        mock_hook_instance = MagicMock()
        mock_ttu_hook.return_value = mock_hook_instance
        mock_hook_instance.execute_bteq.side_effect = AirflowException("BTEQ execution failed")
        context = {}
        with self.assertRaises(AirflowException) as context_manager:
            self.bteq_operator.execute(context)
        self.assertEqual(str(context_manager.exception), "BTEQ execution failed")
