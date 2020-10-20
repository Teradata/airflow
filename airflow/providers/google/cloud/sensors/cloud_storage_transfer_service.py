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
"""This module contains a Google Cloud Transfer sensor."""
from typing import Optional, Sequence, Set, Union

from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import CloudDataTransferServiceHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class CloudDataTransferServiceJobStatusSensor(BaseSensorOperator):
    """
    Waits for at least one operation belonging to the job to have the
    expected status.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataTransferServiceJobStatusSensor`

    :param job_name: The name of the transfer job
    :type job_name: str
    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
    :type expected_statuses: set[str] or string
    :param project_id: (Optional) the ID of the project that owns the Transfer
        Job. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    # [START gcp_transfer_job_sensor_template_fields]
    template_fields = (
        'job_name',
        'impersonation_chain',
    )
    # [END gcp_transfer_job_sensor_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        job_name: str,
        expected_statuses: Union[Set[str], str],
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, str) else expected_statuses
        )
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        hook = CloudDataTransferServiceHook(
            gcp_conn_id=self.gcp_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operations = hook.list_transfer_operations(
            request_filter={'project_id': self.project_id, 'job_names': [self.job_name]}
        )

        check = CloudDataTransferServiceHook.operations_contain_expected_statuses(
            operations=operations, expected_statuses=self.expected_statuses
        )
        if check:
            self.xcom_push(key="sensed_operations", value=operations, context=context)

        return check
