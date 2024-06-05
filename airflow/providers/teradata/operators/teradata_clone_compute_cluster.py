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

from abc import abstractmethod
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, cast, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger
from airflow.providers.teradata.utils.constants import Constants

if TYPE_CHECKING:
    from airflow.utils.context import Context
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _TeradataComputeClusterOperator(BaseOperator):
    """
    Teradata Compute Cluster Base Operator to set up and status operations of compute cluster.

    :param conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = ("conn_id", "timeout")

    ui_color = "#e07c24"

    def __init__(
        self,
        conn_id: str = TeradataHook.default_conn_name,
        timeout: int = Constants.CC_OPR_TIME_OUT,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.timeout = timeout

    @cached_property
    def hook(self) -> TeradataHook:
        return TeradataHook(teradata_conn_id=self.conn_id)

    @abstractmethod
    def execute(self, context: Context):
        pass

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        self._compute_cluster_execute_complete(event)

    def _compute_cluster_execute(self):
        # Verifies if the provided Teradata instance belongs to Vantage Cloud Lake.
        lake_support_find_sql = "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'"
        lake_support_result = self.hook.run(lake_support_find_sql, handler=_single_result_row_handler)
        self.log.info("OFS available - %s ", lake_support_result)
        if lake_support_result is None:
            raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
        # Getting teradata db version. Considering teradata instance is Lake when db version is 20 or above
        db_version_get_sql = "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
        try:
            db_version_result = self.hook.run(db_version_get_sql, handler=_single_result_row_handler)
            if db_version_result is not None:
                db_version_result = str(db_version_result)
                db_version = db_version_result.split(".")[0]
                self.log.info("DBC version %s ", db_version)
                if db_version is not None and int(db_version) < 20:
                    raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
        except Exception as ex:
            self.log.error("Error occurred while getting teradata database version: %s ", str(ex))
            raise

    def _compute_cluster_execute_complete(self, event: dict[str, Any]) -> None:
        if event["status"] == "success":
            self.log.info("Operation Status %s", event["message"])
            return event["message"]
        elif event["status"] == "error":
            raise AirflowException(event["message"])

    def _handle_cc_status(self, operation_type, sql, compute_group_name, compute_profile_name):
        create_sql_result = self._hook_run(sql, handler=_single_result_row_handler)
        self.log.info(
            "%s query ran successfully. Differing to trigger to check status in db. Result from sql: %s",
            operation_type,
            create_sql_result,
        )
        self.defer(
            timeout=timedelta(minutes=self.timeout),
            trigger=TeradataComputeClusterSyncTrigger(
                conn_id=cast(str, self.conn_id),
                compute_profile_name=compute_profile_name,
                compute_group_name=compute_group_name,
                operation_type=operation_type,
                poll_interval=Constants.CC_POLL_INTERVAL,
            ),
            method_name="execute_complete",
        )

        return create_sql_result

    def _hook_run(self, query, handler=None):
        try:
            if handler is not None:
                return self.hook.run(query, handler=handler)
            else:
                return self.hook.run(query)
        except Exception as ex:
            self.log.info(str(ex))
            raise


def _single_result_row_handler(cursor):
    records = cursor.fetchone()
    if isinstance(records, list):
        return records[0]
    if records is None:
        return records
    raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")


def _multiple_result_row_handler(cursor):
    return cursor.fetchall()


def get_compute_group_policy(policy: str):
    try:
        colon_index = policy.find(":")
        query_strategy_quotes = policy[colon_index + 1:]
        start_index = query_strategy_quotes.find('"') + 1
        end_index = query_strategy_quotes.find('"', start_index)
        query_strategy = query_strategy_quotes[start_index:end_index]
    except ValueError:
        query_strategy = None
    return query_strategy


class TeradataCloneComputeClusterProfileOperator(_TeradataComputeClusterOperator):
    """

    Creates a fresh Computer Cluster by duplicating the configuration of the provided Compute Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterProvisionOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param copy_from_compute_profile_name: The name of the Compute Profile from which the configuration
        is replicated to new Compute Profile.
    :param copy_from_compute_group_name: The name of the Compute Group from which the configuration
        is replicated to new Compute Profile.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "copy_from_compute_profile_name",
        "copy_from_compute_group_name"
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        compute_profile_name: str,
        compute_group_name: str,
        copy_from_compute_profile_name: str = '',
        copy_from_compute_group_name: str = '',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.copy_from_compute_profile_name = copy_from_compute_profile_name
        self.copy_from_compute_group_name = copy_from_compute_group_name
        self.compute_group_name = compute_group_name
        self.compute_profile_name = compute_profile_name

    def execute(self, context: Context):
        """
        Initiate the execution of CREATE COMPUTE SQL statement.

        Initiate the execution of the SQL statement for provisioning the compute cluster within Teradata Vantage
        Lake, effectively creates the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        if self.compute_group_name:
            cg_status_query = (
                "SELECT  count(1) FROM DBC.ComputeGroups WHERE ComputeGroupName = '"
                + self.compute_group_name
                + "'"
            )
            cg_status_result = self._hook_run(cg_status_query, _single_result_row_handler)
            if cg_status_result is not None:
                cg_status_result = str(cg_status_result)
            else:
                cg_status_result = 0
            self.log.debug("cg_status_result - %s", cg_status_result)
            if int(cg_status_result) == 0:
                query_get_policy = "SEL ComputeGroupPolicy FROM DBC.ComputeGroups WHERE ComputeGroupName = '" + self.copy_from_compute_group_name + "'"
                query_get_policy_result = self._hook_run(query_get_policy, _single_result_row_handler)
                query_strategy = None
                if query_get_policy_result is not None:
                    query_get_policy_result = str(query_get_policy_result)
                    query_strategy = get_compute_group_policy(query_get_policy_result)
                create_cg_query = "CREATE COMPUTE GROUP " + self.compute_group_name
                if query_strategy is not None:
                    create_cg_query = (
                        create_cg_query + " USING QUERY_STRATEGY ('" + query_strategy + "')"
                    )
                self._hook_run(create_cg_query, _single_result_row_handler)

            cp_status_query = (
                "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE ComputeProfileName = '"
                + self.compute_profile_name
                + "'"
            )
            if self.compute_group_name:
                cp_status_query += " AND ComputeGroupName = '" + self.compute_group_name + "'"
            cp_status_result = self._hook_run(cp_status_query, handler=_single_result_row_handler)
            if cp_status_result is not None:
                cp_status_result = str(cp_status_result)
                msg = f"Compute Profile {self.compute_profile_name} is already exists under Compute Group {self.compute_group_name}. Status is {cp_status_result}"
                self.log.info(msg)
                return cp_status_result
            else:
                show_query = " SHOW COMPUTE PROFILE " + self.copy_from_compute_profile_name
                if self.copy_from_compute_group_name:
                    show_query += " IN " + self.copy_from_compute_group_name
                show_query_result = self._hook_run(show_query, handler=_single_result_row_handler)
                if show_query_result is not None:
                    show_query_result = str(show_query_result)
                    show_query_result = show_query_result.replace(self.copy_from_compute_profile_name,
                                                                  self.compute_profile_name)
                    if not self.compute_group_name and not self.copy_from_compute_group_name:
                        start_index = show_query_result.find("IN") + len("IN")
                        end_index = show_query_result.find(",", start_index)
                        self.compute_group_name = show_query_result[start_index:end_index].strip()
                        show_query_result = show_query_result.replace(self.copy_from_compute_group_name,
                                                                      self.compute_group_name)
                    return self._handle_cc_status(Constants.CC_CREATE_OPR, show_query_result,
                                                  self.compute_group_name, self.compute_profile_name)


class TeradataCloneComputeClusterGroupOperator(_TeradataComputeClusterOperator):
    """

    Creates a fresh Computer Cluster by duplicating the configuration of the provided Compute Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterProvisionOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param copy_from_compute_profile_name: The name of the Compute Profile from which the configuration
        is replicated to new Compute Profile.
    :param copy_from_compute_group_name: The name of the Compute Group from which the configuration
        is replicated to new Compute Profile.
    """

    template_fields: Sequence[str] = (
        "compute_group_name",
        "copy_from_compute_group_name",
        "include_profiles"
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        copy_from_compute_group_name: str,
        compute_group_name: str,
        include_profiles: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.copy_from_compute_group_name = copy_from_compute_group_name
        self.compute_group_name = compute_group_name
        self.include_profiles = include_profiles

    def execute(self, context: Context):
        """
        Initiate the execution of CREATE COMPUTE SQL statement.

        Initiate the execution of the SQL statement for provisioning the compute cluster within Teradata Vantage
        Lake, effectively creates the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        if self.compute_group_name and self.copy_from_compute_group_name:
            cg_status_query = (
                "SELECT  count(1) FROM DBC.ComputeGroups WHERE ComputeGroupName = '"
                + self.compute_group_name
                + "'"
            )
            cg_status_result = self._hook_run(cg_status_query, _single_result_row_handler)
            if cg_status_result is not None:
                cg_status_result = str(cg_status_result)
            else:
                cg_status_result = 0
            self.log.debug("cg_status_result - %s", cg_status_result)
            if int(cg_status_result) == 0:
                query_get_policy = "SEL ComputeGroupPolicy FROM DBC.ComputeGroups WHERE ComputeGroupName = '" + self.copy_from_compute_group_name + "'"
                query_get_policy_result = self._hook_run(query_get_policy, _single_result_row_handler)
                if query_get_policy_result is not None:
                    query_get_policy_result = str(query_get_policy_result)
                    query_strategy = get_compute_group_policy(query_get_policy_result)
                    create_cg_query = "CREATE COMPUTE GROUP " + self.compute_group_name
                    if query_strategy is not None:
                        create_cg_query = (
                            create_cg_query + " USING QUERY_STRATEGY ('" + query_strategy + "')"
                        )
                    self._hook_run(create_cg_query, _single_result_row_handler)
                    if self.include_profiles:
                        get_profile_names_query = (
                            "SEL ComputeProfileName FROM DBC.ComputeProfilesVX WHERE "
                            "ComputeGroupName = '" + self.copy_from_compute_group_name + "'"
                        )
                        get_profile_names_query_result = self._hook_run(get_profile_names_query,
                                                                        handler=_multiple_result_row_handler)
                        for profile_info in get_profile_names_query_result:
                            profile_name = profile_info[0]
                            show_query = " SHOW COMPUTE PROFILE " + profile_name + " IN " + self.copy_from_compute_group_name
                            show_query_result = self._hook_run(show_query, handler=_single_result_row_handler)
                            if show_query_result is not None:
                                show_query_result = str(show_query_result)
                                start_index = show_query_result.find("IN") + len("IN")
                                end_index = show_query_result.find(",", start_index)
                                self.compute_group_name = show_query_result[start_index:end_index].strip()
                                show_query_result = show_query_result.replace(
                                    self.copy_from_compute_group_name,
                                    self.compute_group_name)
                                return self._handle_cc_status(Constants.CC_CREATE_OPR, show_query_result,
                                                              self.compute_group_name, profile_name)
