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

import json
from datetime import datetime
import os
import uuid
from contextlib import closing
from typing import Any
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.models.dag import DAG

try:
    import sqlalchemy
    import teradatasql
    from hooks.teradata import TeradataHook
    # from airflow.providers.teradata.hooks.teradata import TeradataHook
except ImportError:
    pytest.skip("Airflow Provider for Teradata not available, unable to import dependencies sqlalchemy, teradatasql, airflow.providers.teradata.hooks.teradata.TeradataHook", allow_module_level=True)

from airflow.utils import timezone
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestTeradataHook:
    def setup_method(self):
        self.connection = Connection(
            conn_type="teradata",
            login="login",
            password="password",
            host="host",
            schema="schema",
        )
        self.db_hook = TeradataHook(teradata_conn_id='teradata_conn_id', database='test_db')
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestTeradataHook(TeradataHook):
            conn_name_attr = "teradata_conn_id"

            def get_conn(self):
                return conn

        self.test_db_hook = UnitTestTeradataHook()

    @mock.patch("teradatasql.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"

    @mock.patch("sqlalchemy.create_engine")
    def test_get_sqlalchemy_conn(self, mock_connect):
        self.db_hook.get_sqlalchemy_engine()
        assert mock_connect.call_count == 1
        args = mock_connect.call_args.args
        assert len(args) == 1
        expected_link = f'teradatasql://{self.connection.login}:{self.connection.password}@{self.connection.host}'
        assert expected_link == args[0]

    def test_get_connection_form_widgets(self) -> dict[str, Any]:
        ret_val = TeradataHook.get_connection_form_widgets()
        assert len(ret_val) != 0
        assert isinstance(ret_val, dict)

    def test_get_ui_field_behaviour(self) -> dict:
        ret_val = TeradataHook.get_ui_field_behaviour()
        assert len(ret_val) != 0
        assert isinstance(ret_val, dict)

    def test_get_uri(self):
        ret_uri = self.db_hook.get_uri()
        expected_uri = f'teradata://{self.connection.login}:{self.connection.password}@{self.connection.host}/{self.connection.schema}'
        assert expected_uri == ret_uri

    def test_get_records(self):
        sql = "SQL"
        self.test_db_hook.get_records(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_without_parameters(self):
        sql = "SQL"
        self.test_db_hook.run(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_with_parameters(self):
        sql = "SQL"
        param = ("p1", "p2")
        self.test_db_hook.run(sql, parameters=param)
        self.cur.execute.assert_called_once_with(sql, param)
        assert self.conn.commit.called

    def test_insert_rows(self):
        rows = [
            (
                "'test_string",
                None,
                datetime(2023, 8, 15),
                1,
                3.14,
                "str",
            )
        ]
        target_fields = [
            "basestring",
            "none",
            "datetime",
            "int",
            "float",
            "str",
        ]
        self.test_db_hook.insert_rows("table", rows, target_fields)
        self.cur.execute.assert_called_once_with(
            'INSERT INTO table (basestring, none, datetime, int, float, str) VALUES (?,?,?,?,?,?)', ("'test_string", None, '2023-08-15T00:00:00', '1', '3.14', 'str')
        )
