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
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.teradata.hooks.bteq import BteqHook


class TestBteqHook:
    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_execute_bteq(self, mock_popen):
        # Set up mock subprocess
        mock_process = mock.MagicMock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"Query executed successfully\n",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process

        # Set up mock connection
        conn = {
            "host": "localhost",
            "login": "user",
            "password": "password",
            "bteq_output_width": 255,
            "bteq_session_encoding": "UTF8",
            "bteq_quit_zero": True,
            "console_output_encoding": "utf-8",
        }

        # Create hook
        with mock.patch("airflow.providers.teradata.hooks.bteq.BteqHook.get_conn", return_value=conn):
            hook = BteqHook()
            result = hook._execute_bteq_local(
                xcom_push_flag=True,
                timeout=30,
                bteq_file_content="SELECT * FROM table;",
                conn=conn,
            )

            # Verify result
            assert result == "Query executed successfully"
            mock_popen.assert_called_once_with(
                ["bteq"],
                stdin=mock.ANY,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=mock.ANY,
                preexec_fn=os.setsid,
            )

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_execute_bteq_error(self, mock_popen):
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"Failure 3706: Syntax error.\n",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process

        # Set up mock connection
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "localhost",
                "login": "user",
                "password": "password",
                "bteq_output_width": 255,
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            hook = BteqHook()
            with pytest.raises(AirflowException) as exc_info:
                hook.execute_bteq("SELECT * FROM;")
            assert "Failure 3706: Syntax error." in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_execute_bteq_timeout(self, mock_popen):
        # Set up mock subprocess that times out
        mock_process = mock.MagicMock()
        mock_process.wait.side_effect = subprocess.TimeoutExpired("bteq", 5)
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process

        # Set up mock connection
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "localhost",
                "login": "user",
                "password": "password",
                "bteq_output_width": 255,
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            hook = BteqHook()
            with pytest.raises(AirflowException) as exc_info:
                hook.execute_bteq("SELECT * FROM table;", timeout=5)

            assert "timed out after 5 seconds" in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_on_kill(self, mock_popen):
        # Set up mock subprocess
        mock_process = mock.MagicMock()
        mock_popen.return_value = mock_process

        # Set up mock connection
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn", return_value={"sp": mock_process}
        ):
            hook = BteqHook()
            hook.on_kill()

            # Verify process was terminated
            mock_process.terminate.assert_called_once()

    def test_prepare_bteq_script(self):
        hook = BteqHook()
        bteq_script = hook._prepare_bteq_script(
            bteq_string="SELECT * FROM table;",
            host="localhost",
            login="user",
            password="password",
            bteq_output_width=255,
            bteq_session_encoding="UTF8",
            bteq_quit_zero=True,
        )

        # Verify script content
        assert ".LOGON localhost/user,password" in bteq_script
        assert ".SET WIDTH 255" in bteq_script
        assert ".SET SESSION CHARSET 'UTF8'" in bteq_script
        assert "SELECT * FROM table;" in bteq_script
        assert ".QUIT 0" in bteq_script

    def test_prepare_bteq_script_no_quit_zero(self):
        hook = BteqHook()
        bteq_script = hook._prepare_bteq_script(
            bteq_string="SELECT * FROM table;",
            host="localhost",
            login="user",
            password="password",
            bteq_output_width=255,
            bteq_session_encoding="UTF8",
            bteq_quit_zero=False,
        )

        # Verify script content doesn't have .QUIT 0;
        assert ".QUIT 0;" not in bteq_script

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_prepare_bteq_script_validation_errors(self, mock_popen):
        # Test empty BTEQ string
        # Set up mock connection
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"Failure 3706: Syntax error.\n",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "localhost",
                "login": "user",
                "password": "password",
                "bteq_output_width": 255,
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            hook = BteqHook()
            with pytest.raises(ValueError) as exc_info:
                hook.execute_bteq("")
        assert "Host, login, password, and BTEQ script must be provided." in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_prepare_bteq_script_validation_errors_host(self, mock_popen):
        # Test empty BTEQ string
        # Set up mock connection
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "",
                "login": "user",
                "password": "password",
                "bteq_output_width": 255,
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            hook = BteqHook()
            with pytest.raises(ValueError) as exc_info:
                hook.execute_bteq("SELECT * FROM table;")
        assert "Host, login, password, and BTEQ script must be provided." in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_prepare_bteq_script_validation_errors_login(self, mock_popen):
        # Test empty BTEQ string
        # Set up mock connection
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "",
                "login": "user",
                "password": "password",
                "bteq_output_width": 255,
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            # Test missing host
            hook = BteqHook()
            with pytest.raises(ValueError) as exc_info:
                hook.execute_bteq("SELECT * FROM table;")
        assert "Host, login, password, and BTEQ script must be provided." in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_prepare_bteq_script_validation_errors_output_width(self, mock_popen):
        # Test empty BTEQ string
        # Set up mock connection
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "host",
                "login": "user",
                "password": "password",
                "bteq_output_width": "sd",
                "bteq_session_encoding": "UTF8",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            # Test missing host
            hook = BteqHook()
            with pytest.raises(ValueError) as exc_info:
                hook.execute_bteq("SELECT * FROM table;")
        assert "Output width must be a positive integer." in str(exc_info.value)

    @mock.patch("airflow.providers.teradata.hooks.bteq.subprocess.Popen")
    def test_prepare_bteq_script_validation_errors_session(self, mock_popen):
        # Test empty BTEQ string
        # Set up mock connection
        # Set up mock subprocess with error
        mock_process = mock.MagicMock()
        mock_process.returncode = 8
        mock_process.stdout.readline.side_effect = [
            b"Starting BTEQ...\n",
            b"Connected to Teradata\n",
            b"",
            b"",  # Empty line to end the loop
        ]
        mock_popen.return_value = mock_process
        with mock.patch(
            "airflow.providers.teradata.hooks.bteq.BteqHook.get_conn",
            return_value={
                "host": "host",
                "login": "user",
                "password": "password",
                "bteq_output_width": 1,
                "bteq_session_encoding": "",
                "bteq_quit_zero": True,
                "console_output_encoding": "utf-8",
                "sp": mock_process,
            },
        ):
            # Test missing host
            hook = BteqHook()
            with pytest.raises(ValueError) as exc_info:
                hook.execute_bteq("SELECT * FROM table;")
        assert "Session encoding must be specified" in str(exc_info.value)
