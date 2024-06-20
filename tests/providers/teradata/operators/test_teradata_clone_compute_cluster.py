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

from unittest.mock import call, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.teradata.operators.teradata_clone_compute_cluster import \
    TeradataCloneComputeClusterProfileOperator, TeradataCloneComputeClusterGroupOperator
from airflow.providers.teradata.operators.teradata_clone_compute_cluster import (
    _single_result_row_handler,
)
from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger
from airflow.providers.teradata.utils.constants import Constants


@pytest.fixture
def copy_compute_group_name():
    return "copy_test_compute_group"


@pytest.fixture
def copy_compute_profile_name():
    return "copy_test_compute_profile"


@pytest.fixture
def compute_group_name():
    return "test_compute_group"


@pytest.fixture
def compute_profile_name():
    return "test_compute_profile"


@pytest.fixture
def query_strategy():
    return "test_query_strategy"


@pytest.fixture
def compute_map():
    return "test_compute_map"


@pytest.fixture
def compute_attribute():
    return "test_compute_attribute"


@pytest.fixture
def compute_cluster_clone_profile_instance(compute_profile_name, compute_group_name,
                                           copy_compute_profile_name, copy_compute_group_name):
    return TeradataCloneComputeClusterProfileOperator(
        task_id="test", compute_profile_name=compute_profile_name, compute_group_name=compute_group_name,
        copy_from_compute_profile_name=copy_compute_profile_name,
        copy_from_compute_group_name=copy_compute_group_name, conn_id="test_conn"
    )


@pytest.fixture
def compute_cluster_clone_profile_under_same_group_instance(compute_profile_name, copy_compute_profile_name):
    return TeradataCloneComputeClusterProfileOperator(
        task_id="test", compute_profile_name=compute_profile_name,
        copy_from_compute_profile_name=copy_compute_profile_name, conn_id="test_conn"
    )


@pytest.fixture
def compute_cluster_clone_profile_from_other_group_to_user_default_grp_instance(compute_profile_name,
                                                                                compute_group_name,
                                                                                copy_compute_profile_name,
                                                                                copy_compute_group_name):
    return TeradataCloneComputeClusterProfileOperator(
        task_id="test", compute_profile_name=compute_profile_name, compute_group_name=compute_group_name,
        copy_from_compute_profile_name=copy_compute_profile_name,
        copy_from_compute_group_name=copy_compute_group_name, conn_id="test_conn"
    )


@pytest.fixture
def compute_cluster_clone_profile_from_user_default_group_to_other_grp_instance(compute_profile_name,
                                                                                compute_group_name,
                                                                                copy_compute_profile_name,
                                                                                copy_compute_group_name):
    return TeradataCloneComputeClusterProfileOperator(
        task_id="test", compute_profile_name=compute_profile_name, compute_group_name=compute_group_name,
        copy_from_compute_profile_name=copy_compute_profile_name,
        copy_from_compute_group_name=copy_compute_group_name, conn_id="test_conn"
    )


@pytest.fixture
def compute_cluster_clone_group_instance(compute_group_name, copy_compute_group_name):
    return TeradataCloneComputeClusterGroupOperator(
        task_id="test", compute_group_name=compute_group_name,
        copy_from_compute_group_name=copy_compute_group_name, include_profiles=True, conn_id="test_conn"
    )


@pytest.fixture
def compute_cluster_clone_group_no_profiles_instance(compute_group_name, copy_compute_group_name):
    return TeradataCloneComputeClusterGroupOperator(
        task_id="test", compute_group_name=compute_group_name,
        copy_from_compute_group_name=copy_compute_group_name, include_profiles=False, conn_id="test_conn"
    )


class TestTeradataComputeClusterCloneOperator:
    def test_compute_cluster_execute_not_lake(self, compute_cluster_clone_profile_instance):
        with patch.object(compute_cluster_clone_profile_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = [None]
        with pytest.raises(AirflowException):
            compute_cluster_clone_profile_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_check(self, compute_cluster_clone_profile_instance):
        with patch.object(compute_cluster_clone_profile_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "19"]
        with pytest.raises(AirflowException):
            compute_cluster_clone_profile_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_none(self, compute_cluster_clone_profile_instance):
        with patch.object(compute_cluster_clone_profile_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", None]
        with pytest.raises(AirflowException):
            compute_cluster_clone_profile_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_invalid(self, compute_cluster_clone_profile_instance):
        with patch.object(compute_cluster_clone_profile_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "invalid"]
        with pytest.raises(AirflowException):
            compute_cluster_clone_profile_instance._compute_cluster_execute()

    def test_compute_cluster_execute_complete_success(self, compute_cluster_clone_profile_instance):
        event = {"status": "success", "message": "Success message"}
        # Call the method under test
        result = compute_cluster_clone_profile_instance.execute_complete(None, event)
        assert result == "Success message"

    def test_compute_cluster_execute_complete_error(self, compute_cluster_clone_profile_instance):
        event = {"status": "error", "message": "Error message"}
        with pytest.raises(AirflowException):
            compute_cluster_clone_profile_instance.execute_complete(None, event)

    create_profile_sql_when_copying_in_same_group = (
        "CREATE COMPUTE PROFILE copy_test_compute_profile IN test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    expected_create_profile_sql_when_copying_in_same_group = (
        "CREATE COMPUTE PROFILE test_compute_profile IN test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    create_profile_sql_when_copying_from_other_group_to_different_group = (
        "CREATE COMPUTE PROFILE copy_test_compute_profile IN copy_test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    expected_create_profile_sql_when_copying_from_other_group_to_different_group = (
        "CREATE COMPUTE PROFILE test_compute_profile IN test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    create_profile_sql_when_copying_from_default_group_to_different_group = (
        "CREATE COMPUTE PROFILE test_compute_profile IN test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    expected_create_profile_sql_when_copying_from_default_group_to_different_group = (
        "CREATE COMPUTE PROFILE test_compute_profile IN test_compute_group, INSTANCE = TD_COMPUTE_MEDIUM "
        "USING MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')")

    def test_compute_cluster_clone_profile_in_same_group(self, compute_cluster_clone_profile_instance):
        instance = compute_cluster_clone_profile_instance
        with patch.object(instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None, "query_strategy1", None, None,
                                         self.create_profile_sql_when_copying_from_other_group_to_different_group,
                                         "Success"]
            compute_profile_name = instance.compute_profile_name
            compute_group_name = instance.compute_group_name
            copy_compute_group_name = instance.copy_from_compute_group_name
            copy_compute_profile_name = instance.copy_from_compute_profile_name
            with patch.object(instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    conn_id=instance.conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeGroupPolicy FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{copy_compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE GROUP {compute_group_name} USING QUERY_STRATEGY ('query_strategy')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SHOW COMPUTE PROFILE {copy_compute_profile_name} IN {copy_compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            self.expected_create_profile_sql_when_copying_from_other_group_to_different_group,
                            handler=_single_result_row_handler,
                        ),

                    ]
                )

    def test_compute_cluster_existing_group_clone_profile(self, compute_cluster_clone_profile_instance):
        instance = compute_cluster_clone_profile_instance
        with patch.object(instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "1", None,
                                         self.create_profile_sql_when_copying_from_other_group_to_different_group,
                                         "Success"]
            compute_profile_name = instance.compute_profile_name
            compute_group_name = instance.compute_group_name
            copy_compute_group_name = instance.copy_from_compute_group_name
            copy_compute_profile_name = instance.copy_from_compute_profile_name
            with patch.object(instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    conn_id=instance.conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SHOW COMPUTE PROFILE {copy_compute_profile_name} IN {copy_compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            self.expected_create_profile_sql_when_copying_from_other_group_to_different_group,
                            handler=_single_result_row_handler,
                        ),

                    ]
                )

    def test_compute_cluster_clone_profile_from_user_default_group_to_other_grp_instance(self,
                                                                                         compute_cluster_clone_profile_from_user_default_group_to_other_grp_instance):
        instance = compute_cluster_clone_profile_from_user_default_group_to_other_grp_instance
        with patch.object(instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "1", None,
                                         self.create_profile_sql_when_copying_from_default_group_to_different_group,
                                         "Success"]
            compute_profile_name = instance.compute_profile_name
            compute_group_name = instance.compute_group_name
            copy_compute_profile_name = instance.copy_from_compute_profile_name
            copy_compute_group_name = instance.copy_from_compute_group_name
            with patch.object(instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    conn_id=instance.conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SHOW COMPUTE PROFILE {copy_compute_profile_name} IN {copy_compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            self.expected_create_profile_sql_when_copying_from_default_group_to_different_group,
                            handler=_single_result_row_handler,
                        ),

                    ]
                )
