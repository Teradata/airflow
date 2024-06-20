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
"""
Example use of Clone of Teradata Compute Cluster Group
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.models import Param
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers.teradata.operators.teradata_clone_compute_cluster import \
    TeradataCloneComputeClusterProfileOperator, TeradataCloneComputeClusterGroupOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.teradata.operators.teradata_compute_cluster import (
        TeradataComputeClusterProvisionOperator, TeradataComputeClusterDecommissionOperator,
    )
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_vantage_lake_compute_cluster_group_clone_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_computer_cluster_group_clone"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_default", "timeout": 30},
) as dag:
    # Creating a compute cluster
    compute_cluster_provision_operation = TeradataComputeClusterProvisionOperator(
        task_id="compute_cluster_provision_operation",
        compute_profile_name="cp1_group__clone",
        compute_group_name="cg1_group_clone",
        query_strategy="STANDARD",
        compute_map="TD_COMPUTE_XSMALL",
        compute_attribute="MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(1) INITIALLY_SUSPENDED('FALSE')",
    )

    with TaskGroup(group_id='clone_to_existing_group') as clone_to_existing_group:
        with TaskGroup(group_id='clone_to_existing_group_create_cg') as clone_to_existing_group_create_cg:
            create_cg_clone_group = TeradataOperator(
                task_id="create_cg_clone_group",
                sql=r"""
                        CREATE COMPUTE GROUP cg_clone USING QUERY_STRATEGY ('STANDARD')
                    """,
            )

            create_cg_clone_no_profiles_group = TeradataOperator(
                task_id="create_cg_clone_no_profiles_group",
                sql=r"""
                        CREATE COMPUTE GROUP cg_clone_no_profiles USING QUERY_STRATEGY ('STANDARD')
                    """,
            )
        # [START teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_copy_profiles_from_a_group_to_new_group]
        compute_cluster_group_clone_existing_operation_with_profiles = TeradataCloneComputeClusterGroupOperator(
            task_id="compute_cluster_group_clone_operation_with_profiles",
            compute_group_name="cg_clone",
            copy_from_compute_group_name="cg1_group_clone",
            include_profiles=True,
        )
        # [END teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_copy_profiles_from_a_group_to_new_group]

        # [START teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_create_new_group_from_a_group_without_profiles]
        compute_cluster_group_clone_existing_operation_without_profiles = TeradataCloneComputeClusterGroupOperator(
            task_id="compute_cluster_group_clone_operation_without_profiles",
            compute_group_name="cg_clone_no_profiles",
            copy_from_compute_group_name="cg1_group_clone",
            include_profiles=False,
        )
        # [END teradata_vantage_lake_compute_cluster_group_clone_howto_guide_to_create_new_group_from_a_group_without_profiles]
        clone_to_existing_group_create_cg >> [compute_cluster_group_clone_existing_operation_with_profiles,
                                              compute_cluster_group_clone_existing_operation_without_profiles]

    with TaskGroup(group_id='clone_to_new_group') as clone_to_new_group:
        compute_cluster_group_clone_new_operation_with_profiles = TeradataCloneComputeClusterGroupOperator(
            task_id="compute_cluster_group_clone_operation_with_profiles",
            compute_group_name="cg_clone_new",
            copy_from_compute_group_name="cg1_group_clone",
            include_profiles=True,
        )

        compute_cluster_group_clone_new_operation_without_profiles = TeradataCloneComputeClusterGroupOperator(
            task_id="compute_cluster_group_clone_operation_without_profiles",
            compute_group_name="cg_clone_new_no_profiles",
            copy_from_compute_group_name="cg1_group_clone",
            include_profiles=False,
        )

    compute_cluster_provision_operation >> [clone_to_existing_group, clone_to_new_group]

    with TaskGroup(group_id='delete_group') as delete_group:
        with TaskGroup(group_id='delete_profiles_in_group') as delete_profiles_in_group:
            drop_cp1_cg1_group__clone = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_cg1_group__clone",
                compute_profile_name="cp1_group__clone",
                compute_group_name="cg1_group_clone",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )
            drop_cp1_cg_clone = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_cg_clone",
                compute_profile_name="cp1_group__clone",
                compute_group_name="cg_clone",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )

            drop_cp1_cg_clone_new = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_cg_clone_new",
                compute_profile_name="cp1_group__clone",
                compute_group_name="cg_clone_new",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )

            drop_cg_clone_no_profiles = TeradataOperator(
                task_id="drop_cg_clone_no_profiles",
                sql=r"""
                            DROP COMPUTE GROUP cg_clone_no_profiles
                        """,
                trigger_rule=TriggerRule.ALL_DONE
            )

    compute_cluster_provision_operation >> [clone_to_existing_group, clone_to_new_group] >> delete_group

    # [END teradata_vantage_lake_compute_cluster_group_clone_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
