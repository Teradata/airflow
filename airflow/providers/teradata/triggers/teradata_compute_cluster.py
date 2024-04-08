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
import time
from typing import Any, AsyncIterator
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.utils.constants import Constants
from airflow.triggers.base import BaseTrigger, TriggerEvent


class TeradataComputeClusterSyncTrigger(BaseTrigger):

    def __init__(
        self,
        conn_id: str,
        compute_profile_name: str,
        computer_group_name: str | None = None,
        opr_type: str | None = None,
        poll_interval: float | None = None
    ):
        super().__init__()
        self.conn_id = conn_id
        self.compute_profile_name = compute_profile_name
        self.computer_group_name = computer_group_name
        self.opr_type = opr_type
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.teradata.triggers.teradata_compute_cluster.TeradataComputeClusterSyncTrigger",
            {
                "conn_id": self.conn_id,
                "compute_profile_name": self.compute_profile_name,
                "computer_group_name": self.computer_group_name,
                "opr_type": self.opr_type,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = TeradataHook(teradata_conn_id=self.conn_id)
        try:
            while True:
                status = await self.get_status(hook)
                if self.opr_type == Constants.CC_SUSPEND_OPR:
                    if status == Constants.CC_SUSPEND_DB_STATUS:
                        break
                elif self.opr_type == Constants.CC_RESUME_OPR:
                    if status == Constants.CC_RESUME_DB_STATUS:
                        break
                await asyncio.sleep(self.poll_interval)
            if self.opr_type == Constants.CC_SUSPEND_OPR:
                if status == Constants.CC_SUSPEND_DB_STATUS:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": Constants.CC_OPR_SUCCESS_STATUS_MSG % (
                            self.compute_profile_name, {self.opr_type})
                        }
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": Constants.CC_OPR_FAILURE_STATUS_MSG % (
                            self.compute_profile_name, {self.opr_type})
                        }
                    )
            elif self.opr_type == Constants.CC_RESUME_OPR:
                if status == Constants.CC_RESUME_DB_STATUS:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": Constants.CC_OPR_SUCCESS_STATUS_MSG % (
                            self.compute_profile_name, {self.opr_type})
                        }
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": Constants.CC_OPR_FAILURE_STATUS_MSG % (
                            self.compute_profile_name, {self.opr_type})
                        }
                    )
        except Exception as e:
            self.log.info(" custom message %s", str(e))
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def get_status(self, hook: TeradataHook) -> str:

        sql = "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE ComputeProfileName = '" + self.compute_profile_name + "'"
        if self.computer_group_name:
            sql += " AND ComputeGroupName = '" + self.computer_group_name + "'"

        def handler(cursor):
            records = cursor.fetchone()
            if isinstance(records, list):
                return records[0]
            raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")

        return hook.run(sql, handler=handler)
