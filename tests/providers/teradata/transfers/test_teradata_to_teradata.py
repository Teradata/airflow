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

from decimal import Decimal
from unittest import mock
from unittest.mock import MagicMock

import logging

from airflow.providers.teradata.transfers.teradata_to_teradata import TeradataToTeradataOperator


class TestTeradataToTeradataTransfer:
    @mock.patch("airflow.providers.teradata.transfers.teradata_to_teradata.TeradataHook")
    @mock.patch("airflow.providers.teradata.transfers.teradata_to_teradata.TeradataHook")
    def test_execute(self, mock_src_hook, mock_dest_hook):
        dest_teradata_conn_id = "dest_teradata_conn_id"
        destination_table = "destination_table"
        source_teradata_conn_id = "source_teradata_conn_id"
        sql = (r"""select DATE where DATE > {{ sql_params.ref_date }};""",)
        sql_params = {"ref_date": "2024-01-01"}
        rows_chunk = 5000
        cursor_description = [
            ["user_id", Decimal, None, 8, 10, 0, False],
            ["user_name", str, None, 60, None, None, True],
        ]

        cursor_rows = [[Decimal("1"), "User1"], [Decimal("2"), "User2"], [Decimal("3"), "User3"]]

        mock_src_conn = mock_src_hook.get_conn.return_value.__enter__.return_value
        mock_cursor = mock_src_conn.cursor.return_value
        mock_cursor.description.__iter__.return_value = cursor_description
        mock_cursor.fetchmany.side_effect = [cursor_rows, []]
        td_transfer_op = TeradataToTeradataOperator(
            task_id="transfer_data",
            dest_teradata_conn_id=dest_teradata_conn_id,
            destination_table=destination_table,
            source_teradata_conn_id=source_teradata_conn_id,
            sql=sql,
            sql_params=sql_params,
            rows_chunk=rows_chunk,
        )

        td_transfer_op.execute(None)

        assert mock_src_hook.get_conn.called
        assert mock_src_conn.cursor.called
        mock_cursor.execute.assert_called_once_with(sql, sql_params)

        calls = [
            mock.call(rows_chunk),
        ]
        mock_cursor.fetchmany.assert_has_calls(calls)
        mock_dest_hook.bulk_insert_rows.assert_called_once_with(
            destination_table, cursor_rows, commit_every=rows_chunk, target_fields=["user_id", "user_name"]
        )
