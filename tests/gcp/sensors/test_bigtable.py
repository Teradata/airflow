# -*- coding: utf-8 -*-
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

import unittest

import google.api_core.exceptions
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import ClusterState
from parameterized import parameterized

from airflow import AirflowException
from airflow.gcp.sensors.bigtable import BigtableTableReplicationCompletedSensor
from tests.compat import mock

PROJECT_ID = 'test_project_id'
INSTANCE_ID = 'test-instance-id'
GCP_CONN_ID = 'test-gcp-conn-id'
TABLE_ID = 'test-table-id'


class BigtableWaitForTableReplicationTest(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.gcp.sensors.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableTableReplicationCompletedSensor(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.gcp.sensors.bigtable.BigtableHook')
    def test_wait_no_instance(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        op = BigtableTableReplicationCompletedSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.gcp.sensors.bigtable.BigtableHook')
    def test_wait_no_table(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))

        op = BigtableTableReplicationCompletedSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.gcp.sensors.bigtable.BigtableHook')
    def test_wait_not_ready(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.return_value = {
            "cl-id": ClusterState(0)
        }
        op = BigtableTableReplicationCompletedSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.gcp.sensors.bigtable.BigtableHook')
    def test_wait_ready(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.return_value = {
            "cl-id": ClusterState(4)
        }
        op = BigtableTableReplicationCompletedSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertTrue(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
