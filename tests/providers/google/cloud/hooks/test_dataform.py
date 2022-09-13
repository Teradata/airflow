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

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.dataform import DataformHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAFORM_STRING = "airflow.providers.google.cloud.hooks.dataform.{}"

PROJECT_ID = "project-id"
REGION = "region"
REPOSITORY_ID = "test_repository"
WORKSPACE_ID = "test_workspace"
GCP_CONN_ID = "google_cloud_default"
DELEGATE_TO = "test-delegate-to"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
COMPILATION_RESULT = {
    "git_commitish": "main",
    "workspace": (
        f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"
    ),
}
COMPILATION_RESULT_ID = "test_compilation_result_id"
WORKFLOW_INVOCATION = {
    "compilation_result": (
        f"projects/{PROJECT_ID}/locations/{REGION}/repositories/"
        f"{REPOSITORY_ID}/compilationResults/{COMPILATION_RESULT_ID}"
    ),
}
WORKFLOW_INVOCATION_ID = "test_workflow_invocation_id"


class TestDataflowHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = DataformHook(
                gcp_conn_id=GCP_CONN_ID,
                delegate_to=DELEGATE_TO,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(DATAFORM_STRING.format("DataformHook.get_dataform_client"))
    def test_create_compilation_result(self, mock_client):
        self.hook.create_compilation_result(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result=COMPILATION_RESULT,
        )
        parent = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}"
        mock_client.return_value.create_compilation_result.assert_called_once_with(
            request=dict(parent=parent, compilation_result=COMPILATION_RESULT),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAFORM_STRING.format("DataformHook.get_compilation_result"))
    def get_compilation_result(self, mock_client):
        self.hook.create_compilation_result(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
        )
        name = (
            f"projects/{PROJECT_ID}/locations/{REGION}/repositories/"
            f"{REPOSITORY_ID}/compilationResults/{COMPILATION_RESULT_ID}"
        )
        mock_client.return_value.get_compilation_result.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAFORM_STRING.format("DataformHook.get_dataform_client"))
    def test_create_workflow_invocation(self, mock_client):
        self.hook.create_workflow_invocation(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation=WORKFLOW_INVOCATION,
        )
        parent = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}"
        mock_client.return_value.create_workflow_invocation.assert_called_once_with(
            request=dict(parent=parent, workflow_invocation=WORKFLOW_INVOCATION),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAFORM_STRING.format("DataformHook.get_dataform_client"))
    def test_get_workflow_invocation(self, mock_client):
        self.hook.get_workflow_invocation(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
        )
        name = (
            f"projects/{PROJECT_ID}/locations/{REGION}/repositories/"
            f"{REPOSITORY_ID}/workflowInvocations/{WORKFLOW_INVOCATION_ID}"
        )
        mock_client.return_value.get_workflow_invocation.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAFORM_STRING.format("DataformHook.get_dataform_client"))
    def test_cancel_workflow_invocation(self, mock_client):
        self.hook.cancel_workflow_invocation(
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=WORKFLOW_INVOCATION_ID,
        )
        name = (
            f"projects/{PROJECT_ID}/locations/{REGION}/repositories/"
            f"{REPOSITORY_ID}/workflowInvocations/{WORKFLOW_INVOCATION_ID}"
        )
        mock_client.return_value.cancel_workflow_invocation.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
