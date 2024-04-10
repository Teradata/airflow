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

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.utils.constants import Constants

if TYPE_CHECKING:
    from airflow.utils.context import Context

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence, cast

from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException


class TeradataComputeClusterSuspendOperator(BaseOperator):
    """
    Teradata Compute Cluster Operator to suspend the specified Teradata Vantage Cloud Lake Compute Cluster.

    Suspends the Teradata Vantage Lake Computer Cluster by employing the SUSPEND SQL statement within the
    Teradata Vantage Lake Compute Cluster SQL Interface.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param computer_group_name: Name of compute group to which compute profile belongs.
    :param conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = ("compute_profile_name", "computer_group_name", "conn_id", "timeout")

    ui_color = "#e07c24"

    def __init__(
        self,
        compute_profile_name: str,
        computer_group_name: str | None = None,
        conn_id: str = TeradataHook.default_conn_name,
        timeout: int = Constants.CC_OPR_TIME_OUT,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_profile_name = compute_profile_name
        self.computer_group_name = computer_group_name
        self.conn_id = conn_id
        self.timeout = timeout

    def execute(self, context: Context):
        """
        Initiates the execution of the SQL statement for resuming the compute cluster within Teradata Vantage
        Lake, effectively resuming the compute cluster.

        Airflow runs this method on the worker and defers using the trigger.

        """
        return compute_cluster_execute(self, Constants.CC_SUSPEND_OPR)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        compute_cluster_execute_complete(self, event)


class TeradataComputeClusterResumeOperator(BaseOperator):
    """
    Teradata Compute Cluster Operator to Resume the specified Teradata Vantage Cloud Lake Compute Cluster.

    Resumes the Teradata Vantage Lake Computer Cluster by employing the RESUME SQL statement within the
    Teradata Vantage Lake Compute Cluster SQL Interface.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param computer_group_name: Name of compute group to which compute profile belongs.
    :param conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = ("compute_profile_name", "computer_group_name", "conn_id", "timeout")

    ui_color = "#e07c24"

    def __init__(
        self,
        compute_profile_name: str,
        computer_group_name: str | None = None,
        conn_id: str = TeradataHook.default_conn_name,
        timeout: int = Constants.CC_OPR_TIME_OUT,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_profile_name = compute_profile_name
        self.computer_group_name = computer_group_name
        self.conn_id = conn_id
        self.timeout = timeout

    def execute(self, context: Context):
        """
        Initiates the execution of the SQL statement for resuming the compute cluster within Teradata Vantage
        Lake, effectively resuming the compute cluster.

        Airflow runs this method on the worker and defers using the trigger.

        """
        return compute_cluster_execute(self, Constants.CC_RESUME_OPR)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        self.log.info("Response came after trigger execute ")
        compute_cluster_execute_complete(self, event)


def compute_cluster_execute(self, opr):
    hook = TeradataHook(teradata_conn_id=self.conn_id)
    # Verifies the provided compute profile name.
    if (
        self.compute_profile_name is None
        or self.compute_profile_name == "None"
        or self.compute_profile_name == ""
    ):
        self.log.info("Invalid compute cluster profile name")
        raise AirflowException(Constants.CC_OPR_EMPTY_PROFILE_ERROR_MSG)
    # Verifies if the provided Teradata instance belongs to Vantage Cloud Lake.
    sql = "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'"
    result = hook.run(sql, handler=__handler)
    self.log.info("OFS available - %s ", result)
    if int(result) == 0:
        raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)

    sql = "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
    result = hook.run(sql, handler=__handler)
    db_version = result.split(".")[0]
    self.log.info("DBC version %s ", db_version)
    if int(db_version) < 20:
        raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)

    sql = (
        "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE ComputeProfileName = '"
        + self.compute_profile_name
        + "'"
    )
    if self.computer_group_name:
        sql += " AND ComputeGroupName = '" + self.computer_group_name + "'"
    result = hook.run(sql, handler=__handler)
    # Generates an error message if the compute cluster does not exist for the specified compute profile and compute group.
    if result is None:
        self.log.info(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
        raise AirflowException(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
    # Generates an error message if the compute cluster initializing
    if result == Constants.CC_INITIALIZE_DB_STATUS:
        self.log.info(Constants.CC_OPR_INITIALIZING_STATUS_MSG)
        raise AirflowException(Constants.CC_OPR_INITIALIZING_STATUS_MSG)
    # SUSPEND operation
    if opr == Constants.CC_SUSPEND_OPR:
        # If status is not in SUSPEND
        if result != Constants.CC_SUSPEND_DB_STATUS:
            sql = f"SUSPEND COMPUTE FOR COMPUTE PROFILE {self.compute_profile_name}"
            if self.computer_group_name:
                sql = f"{sql} IN COMPUTE GROUP {self.computer_group_name}"
            self.log.info(f"Compute Cluster {opr} Operation - SQL : %s", sql)
            return __handle_result(
                self,
                Constants.CC_SUSPEND_OPR,
                Constants.CC_SUSPEND_DB_STATUS,
                Constants.CC_RESUME_DB_STATUS,
                sql,
                result,
                hook,
            )
        else:
            self.log.info(
                "Compute Cluster %s already %s", self.compute_profile_name, Constants.CC_SUSPEND_OPR
            )
    # RESUME operation
    elif opr == Constants.CC_RESUME_OPR:
        # If status is not in RESUME
        if result != Constants.CC_RESUME_DB_STATUS:
            sql = f"RESUME COMPUTE FOR COMPUTE PROFILE {self.compute_profile_name}"
            if self.computer_group_name:
                sql = f"{sql} IN COMPUTE GROUP {self.computer_group_name}"
            self.log.info(f"Compute Cluster {opr} Operation - SQL : %s", sql)
            return __handle_result(
                self,
                Constants.CC_RESUME_OPR,
                Constants.CC_RESUME_DB_STATUS,
                Constants.CC_SUSPEND_DB_STATUS,
                sql,
                result,
                hook,
            )
        else:
            self.log.info("Compute Cluster %s already %s", self.compute_profile_name, Constants.CC_RESUME_OPR)


def compute_cluster_execute_complete(self, event: dict[str, Any]) -> None:
    if event["status"] == "success":
        self.log.info("Operation Status %s", event["message"])
    elif event["status"] == "error":
        raise AirflowException(event["message"])


def __handle_result(self, opr_type, db_status, check_opp_db_status, sql, result, hook):
    try:
        hook.run(sql)
    except Exception as ex:
        ignored = False
        # Handling if operation is already in progress
        if "[Error 4825]" in str(ex) and result == check_opp_db_status:
            self.log.info(f"A {opr_type} operation is already underway. Kindly check the status.")
            self.ignored = True
            return False
        # Handling permission issue errors
        if "[Error 4824]" in str(ex):
            self.log.info(Constants.CC_GRP_PRP_UN_AUTHORIZED_MSG, opr_type)
            raise AirflowException(Constants.CC_GRP_PRP_UN_AUTHORIZED_MSG, opr_type)
        if not ignored:
            raise  # rethrow
    self.log.info(f"{opr_type} query ran successfully. Differing to trigger to check status in db")
    self.defer(
        timeout=timedelta(seconds=self.timeout),
        trigger=TeradataComputeClusterSyncTrigger(
            conn_id=cast(str, self.conn_id),
            compute_profile_name=self.compute_profile_name,
            computer_group_name=self.computer_group_name,
            opr_type=opr_type,
            poll_interval=Constants.CC_POLL_INTERVAL,
        ),
        method_name="execute_complete",
    )


def __handler(cursor):
    records = cursor.fetchone()
    if isinstance(records, list):
        return records[0]
    if records is None:
        return records
    raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")
