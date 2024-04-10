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
Example use of Teradata Compute Cluster Resume Operator
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.models import Param

try:
    from airflow.providers.teradata.operators.teradata_compute_cluster import (
        TeradataComputeClusterResumeOperator,
    )
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_vantage_lake_compute_cluster_manage_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_computer_cluster_resume"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_lake"},
    render_template_as_native_obj=True,
    params={
        "computer_group_name": Param(
            "compute_group_test",
            type="string",
            title="Compute cluster group Name:",
            description="Enter compute cluster group name.",
        ),
        "compute_profile_name": Param(
            "compute_profile_test",
            type="string",
            title="Compute cluster profile Name:",
            description="Enter compute cluster profile name.",
        ),
        "conn_id": Param(
            "teradata_lake",
            type="string",
            title="Teradata ConnectionId:",
            description="Enter Teradata connection id.",
        ),
        "timeout": Param(
            20,
            type="integer",
            title="Timeout:",
            description="Time elapsed before the task times out and fails.",
        ),
    },
) as dag:
    # [START teradata_compute_cluster_resume_operator_howto_guide]
    compute_cluster_resume_operation = TeradataComputeClusterResumeOperator(
        task_id="compute_cluster_resume_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        computer_group_name="{{ params.computer_group_name }}",
        conn_id="{{ params.conn_id }}",
        timeout="{{ params.timeout }}",
    )
    # [END teradata_compute_cluster_resume_operator_howto_guide]
    (compute_cluster_resume_operation)

    # [END teradata_vantage_lake_compute_cluster_manage_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
