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

import asyncio
import re
from typing import Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.utils.constants import Constants
from airflow.triggers.base import BaseTrigger, TriggerEvent


def _handler(cursor):
    records = cursor.fetchone()
    if isinstance(records, list):
        return records[0]
    if records is None:
        return records
    raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")


class TeradataCloneComputeClusterSyncTrigger(BaseTrigger):
    """
    Fetch the status of the suspend or resume operation for the specified compute cluster.

    :param conn_id:  The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param li_compute_profile:  The list of Compute Cluster Profiles to be cloned from the
        copy_from_compute_group_name to the compute_group_name.
    :param compute_group_name: The compute group name represents the destination
        where the copied compute profiles will be placed
    :param copy_from_compute_group_name: The name of the parent Compute Cluster Group
        serves as the source for cloning profiles.
    :param poll_interval: polling period in minutes to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        li_compute_profile: list,
        copy_from_compute_group_name: str,
        compute_group_name: str,
        poll_interval: float | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.li_compute_profile = li_compute_profile
        self.copy_from_compute_group_name = copy_from_compute_group_name
        self.compute_group_name = compute_group_name
        self.poll_interval = poll_interval
        if self.poll_interval is not None:
            self.poll_interval = float(self.poll_interval)
        else:
            self.poll_interval = float(Constants.CC_POLL_INTERVAL)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize TeradataCloneComputeClusterSyncTrigger arguments and classpath."""
        return (
            "airflow.providers.teradata.triggers.teradata_clone_compute_cluster.TeradataCloneComputeClusterSyncTrigger",
            {
                "conn_id": self.conn_id,
                "li_compute_profile": self.li_compute_profile,
                "compute_group_name": self.compute_group_name,
                "copy_from_compute_group_name": self.copy_from_compute_group_name,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            hook = TeradataHook()
            result = await self.clone_profiles(hook)
            if result == 'success':
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": "Compute Cluster cloned successfully",
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": "Error Occurred while cloning compute cluster",
                    }
                )
        except Exception as e:
            self.log.error(str(e))
            yield {"status": "error", "message": str(e)}
        except asyncio.CancelledError:
            self.log.error(Constants.CC_OPR_TIMEOUT_ERROR, "Clone")

    async def clone_profiles(self, hook: TeradataHook) -> str:
        """
        Asynchronously clone multiple compute profiles.
        """
        status = 'failure'
        try:
            # Create async tasks in a for loop
            create_tasks = [self.create_cluster(hook, profile_name) for profile_name in
                            self.li_compute_profile]
            create_results = await asyncio.gather(*create_tasks)
            if len(self.li_compute_profile) == len(create_results):
                tasks = [self.check_status(hook, profile_name) for profile_name in self.li_compute_profile]
                status_results = await asyncio.gather(*tasks)
                if len(self.li_compute_profile) == len(status_results):
                    if 'failure' not in status_results:
                        status = 'success'
        except Exception as e:
            self.log.error(str(e))
        return status

    async def create_cluster(self, hook: TeradataHook, profile_name) -> str:
        status = 'failure'
        try:
            show_query = "SHOW COMPUTE PROFILE " + profile_name + " IN " + self.copy_from_compute_group_name
            show_query_result = hook.run(show_query, handler=_handler)
            if show_query_result is not None:
                show_query_result = str(show_query_result)
                show_query_result = re.sub(re.escape(self.copy_from_compute_group_name),
                                           self.compute_group_name, show_query_result,
                                           flags=re.IGNORECASE)
                hook.run(show_query_result, handler=_handler)
                status = 'success'
        except Exception as e:
            self.log.error(str(e))
        return status

    async def check_status(self, hook: TeradataHook, profile_name) -> str:
        status = "failure"
        try:
            while True:
                status = await self.get_status(hook, profile_name)
                if status is None or len(status) == 0:
                    raise AirflowException(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
                if status == Constants.CC_RESUME_DB_STATUS:
                    break
                if self.poll_interval is not None:
                    self.poll_interval = float(self.poll_interval)
                else:
                    self.poll_interval = float(Constants.CC_POLL_INTERVAL)
                await asyncio.sleep(self.poll_interval)
            if status == Constants.CC_RESUME_DB_STATUS:
                status = "success"
        except Exception as e:
            self.log.error(str(e))
        except asyncio.CancelledError:
            self.log.info(Constants.CC_OPR_TIMEOUT_ERROR, "Clone")
        return status

    async def get_status(self, hook: TeradataHook, compute_profile_name) -> str:
        """Return compute cluster SUSPEND/RESUME operation status."""
        sql = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE ComputeProfileName = '"
            + compute_profile_name
            + "'"
        )
        if self.compute_group_name:
            sql += " AND ComputeGroupName = '" + self.compute_group_name + "'"
        return str(hook.run(sql, handler=_handler))
