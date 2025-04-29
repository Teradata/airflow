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
from airflow.providers.teradata.hooks.ttu import TtuHook


class TestTtuHook(unittest.TestCase):
    def setUp(self):
        self.ttu_hook = TtuHook(ttu_conn_id="teradata_default")
        self.connection = MagicMock()
        self.connection.login = "test_user"
        self.connection.password = "test_password"
        self.connection.host = "test_host"
        self.connection.extra_dejson = {
            "ttu_log_folder": "/tmp",
            "console_output_encoding": "utf-8",
            "bteq_session_encoding": "UTF8",
            "bteq_output_width": 65531,
            "bteq_quit_zero": True,
        }

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_get_conn(self, mock_get_connection):
        mock_get_connection.return_value = self.connection
        conn = self.ttu_hook.get_conn()
        self.assertEqual(conn["login"], "test_user")
        self.assertEqual(conn["password"], "test_password")
        self.assertEqual(conn["host"], "test_host")
        self.assertEqual(conn["ttu_log_folder"], "/tmp")
        self.assertEqual(conn["bteq_session_encoding"], "UTF8")

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_get_conn_missing_params(self, mock_get_connection):
        self.connection.login = None
        mock_get_connection.return_value = self.connection
        with self.assertRaises(AirflowException):
            self.ttu_hook.get_conn()

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_close_conn(self, mock_get_connection):
        mock_get_connection.return_value = self.connection
        # Call get_conn to initialize self.ttu_hook.conn
        conn = self.ttu_hook.get_conn()
        # Ensure self.ttu_hook.conn is not None before proceeding
        self.assertIsNotNone(conn, "Connection was not initialized")
        conn["sp"] = MagicMock()
        self.ttu_hook.close_conn()
        # Access self.ttu_hook.conn only if it's not None
        if self.ttu_hook.conn:
            self.ttu_hook.conn["sp"].terminate.assert_called()

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    @patch("subprocess.Popen")
    def test_execute_bteq(self, mock_popen, mock_get_connection):
        mock_get_connection.return_value = self.connection
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [b"test_output\n", b""]
        mock_popen.return_value = mock_process

        result = self.ttu_hook.execute_bteq("SELECT * FROM test_table;")
        self.assertEqual(result, None)

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    @patch("subprocess.Popen")
    def test_execute_bteq_xcom_push(self, mock_popen, mock_get_connection):
        mock_get_connection.return_value = self.connection
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [b"test_output\n", b""]
        mock_popen.return_value = mock_process

        result = self.ttu_hook.execute_bteq("SELECT * FROM test_table;", xcom_push_flag=True)
        self.assertEqual(result, "test_output")

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    @patch("subprocess.Popen")
    def test_execute_bteq_failure(self, mock_popen, mock_get_connection):
        mock_get_connection.return_value = self.connection
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.stdout.readline.side_effect = [b"Failure occurred\n", b""]
        mock_popen.return_value = mock_process

        with self.assertRaises(AirflowException):
            self.ttu_hook.execute_bteq("SELECT * FROM test_table;")

    @patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_on_kill(self, mock_get_connection):
        mock_get_connection.return_value = self.connection
        # Call get_conn to initialize self.ttu_hook.conn
        conn = self.ttu_hook.get_conn()
        # Ensure self.ttu_hook.conn is not None before proceeding
        self.assertIsNotNone(conn, "Connection was not initialized")
        conn["sp"] = MagicMock()
        self.ttu_hook.on_kill()
        # Access self.ttu_hook.conn only if it's not None
        if self.ttu_hook.conn:
            self.ttu_hook.conn["sp"].terminate.assert_called()

    def test_prepare_bteq_script(self):
        bteq_script = "SELECT * FROM test_table;"
        prepared_script = self.ttu_hook._prepare_bteq_script(
            bteq_string=bteq_script,
            host="test_host",
            login="test_user",
            password="test_password",
            bteq_output_width=65531,
            bteq_session_encoding="UTF8",
            bteq_quit_zero=True,
        )
        self.assertIn(".LOGON test_host/test_user,test_password;", prepared_script)
        self.assertIn(".SET WIDTH 65531;", prepared_script)
        self.assertIn(".SET SESSION CHARSET 'UTF8';", prepared_script)
        self.assertIn("SELECT * FROM test_table;", prepared_script)
        self.assertIn(".QUIT 0;", prepared_script)
        self.assertIn(".LOGOFF;", prepared_script)
        self.assertIn(".EXIT;", prepared_script)

    def test_prepare_bteq_script_empty(self):
        with self.assertRaises(ValueError):
            self.ttu_hook._prepare_bteq_script(
                bteq_string="",
                host="test_host",
                login="test_user",
                password="test_password",
                bteq_output_width=65531,
                bteq_session_encoding="UTF8",
                bteq_quit_zero=True,
            )
