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
Example use of Teradata Compute Cluster Provision Operator
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.teradata.operators.teradata_compute_cluster import (
        TeradataComputeClusterDecommissionOperator,
        TeradataComputeClusterProvisionOperator,
        TeradataComputeClusterResumeOperator,
        TeradataComputeClusterSuspendOperator,
    )
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_vantage_lake_compute_cluster_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_computer_cluster"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": "teradata_lake", "timeout": "40"},
    render_template_as_native_obj=True,
) as dag:
    with TaskGroup(
        "cc_initially_run_example", tooltip="Tasks of compute cluster with initially run"
    ) as cc_initially_run_example:
        # [START teradata_vantage_lake_compute_cluster_provision_howto_guide]
        compute_cluster_provision_operation = TeradataComputeClusterProvisionOperator(
            task_id="compute_cluster_provision_operation",
            compute_profile_name="compute_profile_test",
            compute_group_name="compute_group_test",
            query_strategy="STANDARD",
            compute_map="TD_COMPUTE_XSMALL",
            compute_attribute="MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(1) INITIALLY_SUSPENDED('FALSE')",
        )
        # [END teradata_vantage_lake_compute_cluster_provision_howto_guide]
        # [START teradata_vantage_lake_compute_cluster_suspend_howto_guide]
        compute_cluster_suspend_operation = TeradataComputeClusterSuspendOperator(
            task_id="compute_cluster_suspend_operation",
            compute_profile_name="compute_profile_test",
            compute_group_name="compute_group_test",
        )
        # [END teradata_vantage_lake_compute_cluster_suspend_howto_guide]
        # [START teradata_vantage_lake_compute_cluster_decommission_howto_guide]
        compute_cluster_decommission_operation = TeradataComputeClusterDecommissionOperator(
            task_id="compute_cluster_decommission_operation",
            compute_profile_name="compute_profile_test",
            compute_group_name="compute_group_test",
            delete_compute_group=True,
        )
        # [END teradata_vantage_lake_compute_cluster_decommission_howto_guide]
        (
            compute_cluster_provision_operation
            >> compute_cluster_suspend_operation
            >> compute_cluster_decommission_operation
        )
    with TaskGroup(
        "cc_initially_suspend_example", tooltip="Tasks of compute cluster with initially suspend"
    ) as cc_initially_suspend_example:
        # [START teradata_vantage_lake_compute_cluster_provision_initially_suspend_howto_guide]
        compute_cluster_provision_initially_suspend_operation = TeradataComputeClusterProvisionOperator(
            task_id="compute_cluster_provision_initially_suspend_operation",
            compute_profile_name="compute_profile_test_is",
            compute_group_name="compute_group_test_is",
            query_strategy="STANDARD",
            compute_map="TD_COMPUTE_XSMALL",
            compute_attribute="MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(1) INITIALLY_SUSPENDED('TRUE')",
        )
        # [END teradata_vantage_lake_compute_cluster_provision_initially_suspend_howto_guide]
        # [START teradata_vantage_lake_compute_cluster_resume_howto_guide]
        compute_cluster_initially_suspend_resume_operation = TeradataComputeClusterResumeOperator(
            task_id="compute_cluster_initially_suspend_resume_operation",
            compute_profile_name="compute_profile_test_is",
            compute_group_name="compute_group_test_is",
        )
        # [END teradata_vantage_lake_compute_cluster_resume_howto_guide]
        compute_cluster_initially_suspend_decommission_operation = TeradataComputeClusterDecommissionOperator(
            task_id="compute_cluster_initially_suspend_decommission_operation",
            compute_profile_name="compute_profile_test_is",
            compute_group_name="compute_group_test_is",
            delete_compute_group=True,
        )
        (
            compute_cluster_provision_initially_suspend_operation
            >> compute_cluster_initially_suspend_resume_operation
            >> compute_cluster_initially_suspend_decommission_operation
        )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
