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
Example use of Clone of Teradata Compute Cluster Profile
"""

from __future__ import annotations

import datetime
import logging
import os

import pytest

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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

# [START teradata_vantage_lake_compute_cluster_profile_clone_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_computer_cluster_profile_clone"

logger = logging.getLogger(__name__)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_default"},
    render_template_as_native_obj=True,
    params={
        "compute_group_name": Param(
            "cg1",
            type="string",
            title="Compute cluster group Name:",
            description="Enter compute cluster group name.",
        ),
        "compute_profile_name": Param(
            "cp1",
            type="string",
            title="Compute cluster profile Name:",
            description="Enter compute cluster profile name.",
        ),
        "copy_from_compute_group_name": Param(
            "cg1",
            type="string",
            title="Parent compute cluster group Name:",
            description="Enter parent compute cluster group name.",
        ),
        "copy_from_compute_profile_name": Param(
            "cp1",
            type="string",
            title="Parent compute cluster profile Name:",
            description="Enter parent compute cluster profile name.",
        ),
        "conn_id": Param(
            "teradata_default",
            type="string",
            title="Teradata ConnectionId:",
            description="Enter Teradata connection id.",
        ),
        "timeout": Param(
            35,
            type="integer",
            title="Timeout:",
            description="Time elapsed before the task times out and fails. Timeout is in minutes.",
        ),
    },
) as dag:
    # Creating a compute cluster
    compute_cluster_provision_operation = TeradataComputeClusterProvisionOperator(
        task_id="compute_cluster_provision_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        compute_group_name="{{ params.compute_group_name }}",
        conn_id="{{ params.conn_id }}",
        timeout="{{ params.timeout }}",
        query_strategy="STANDARD",
        compute_map="TD_COMPUTE_XSMALL",
        compute_attribute="MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(1) INITIALLY_SUSPENDED('FALSE')",
    )

    with TaskGroup(group_id='with_in_group') as tg0:
        # [START teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_from_other_profile_in_same_group]
        compute_cluster_profile_clone_operation_same_group_different_profile = TeradataCloneComputeClusterProfileOperator(
            task_id="compute_cluster_profile_clone_operation_same_group_different_profile",
            compute_profile_name="cp2",
            compute_group_name="cg1",
            copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
            copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}"
        )
        # [END teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_from_other_profile_in_same_group]

    # Clone compute clusters to existing group
    with TaskGroup(group_id='target_group_exists') as tg1:
        create_cg2 = TeradataOperator(
            task_id="create_compute_group_cg2",
            sql=r"""
                CREATE COMPUTE GROUP cg2 USING QUERY_STRATEGY ('STANDARD')
                """,
        )
        # [START teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_in_another_group_from_a_profile_in_a_group]
        compute_cluster_profile_clone_operation_different_existing_group_different_profile = TeradataCloneComputeClusterProfileOperator(
            task_id="compute_cluster_profile_clone_operation_different_existing_group_different_profile",
            compute_profile_name="cp2",
            compute_group_name="cg2",
            copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
            copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}"
        )
        # [END teradata_vantage_lake_compute_cluster_profile_clone_howto_guide_to_create_new_profile_in_another_group_from_a_profile_in_a_group]

        compute_cluster_profile_clone_operation_different_existing_group_same_profile = TeradataCloneComputeClusterProfileOperator(
            task_id="compute_cluster_profile_clone_operation_different_existing_group_same_profile",
            compute_profile_name="cp1",
            compute_group_name="cg2",
            copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
            copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}"
        )

        (
            create_cg2
            >> [compute_cluster_profile_clone_operation_different_existing_group_different_profile,
                compute_cluster_profile_clone_operation_different_existing_group_same_profile]
        )

    with TaskGroup(group_id='target_group_new') as tg2:
        # Clone compute clusters to new compute group

        compute_cluster_profile_clone_operation_different_new_group_different_profile = TeradataCloneComputeClusterProfileOperator(
            task_id="compute_cluster_profile_clone_operation_different_new_group_different_profile",
            compute_profile_name="cp2",
            compute_group_name="cg3",
            copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
            copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}"
        )

        compute_cluster_profile_clone_operation_different_new_group_same_profile = TeradataCloneComputeClusterProfileOperator(
            task_id="compute_cluster_profile_clone_operation_different_new_group_same_profile",
            compute_profile_name="cp1",
            compute_group_name="cg4",
            copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
            copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}"
        )

    with TaskGroup(group_id='default_group') as tg3:
        # - Default Group Operations
        # - Default Group as target

        def access_connection(conid, **kwargs):
            # Accessing connection by its name
            connection = BaseHook.get_connection(conid)
            ti = kwargs['ti']
            print("User logged - %s", connection.login)
            logger.info("User logged - %s", connection.login)
            ti.xcom_push(key='user', value=connection.login)


        access_connection_task = PythonOperator(
            task_id='access_connection_task',
            python_callable=access_connection,
            op_kwargs={"conid": "{{ params.conn_id }}"},
            provide_context=True
        )

        create_default_group = TeradataOperator(
            task_id="create_default_group",
            sql=r"""
                    CREATE COMPUTE GROUP dg USING QUERY_STRATEGY ('STANDARD')
                """,
        )

        assign_default_group_to_user = TeradataOperator(
            task_id="assign_default_group_to_user",
            sql=r"""MODIFY USER {{ task_instance.xcom_pull(task_ids='default_group.access_connection_task', key='user') }} AS
                        COMPUTE GROUP = dg """,
        )

        with TaskGroup(group_id='default_target_group') as tg31:
            compute_cluster_profile_clone_operation_to_default_group_same_profile = TeradataCloneComputeClusterProfileOperator(
                task_id="compute_cluster_profile_clone_operation_default_group_same_profile",
                compute_profile_name="cp1",
                compute_group_name="dg",
                copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
                copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}"
            )

            compute_cluster_profile_clone_operation_to_default_group_different_profile = TeradataCloneComputeClusterProfileOperator(
                task_id="compute_cluster_profile_clone_operation_different_group_same_profile",
                compute_profile_name="cp2",
                compute_group_name="dg",
                copy_from_compute_profile_name="{{ params.copy_from_compute_profile_name }}",
                copy_from_compute_group_name="{{ params.copy_from_compute_group_name }}",
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}"
            )

        # - Default Group as source

        with TaskGroup(group_id='default_source_group') as tg32:
            compute_cluster_profile_clone_operation_from_default_group_same_profile = TeradataCloneComputeClusterProfileOperator(
                task_id="compute_cluster_profile_clone_operation_from_default_group_same_profile",
                compute_profile_name="cp1",
                compute_group_name="cg5",
                copy_from_compute_profile_name="cp1",
                copy_from_compute_group_name="dg",
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}"
            )

            compute_cluster_profile_clone_operation_from_default_group_different_profile = TeradataCloneComputeClusterProfileOperator(
                task_id="compute_cluster_profile_clone_operation_from_default_group_different_profile",
                compute_profile_name="cp2",
                compute_group_name="cg6",
                copy_from_compute_profile_name="cp1",
                copy_from_compute_group_name="dg",
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}"
            )

        (
            access_connection_task
            >> create_default_group
            >> assign_default_group_to_user
            >> tg31
            >> tg32
        )

    with TaskGroup(group_id='delete_group') as tg4:
        with TaskGroup(group_id='delete_cg1') as tg41:
            drop_cp1_cg1 = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_cg1",
                compute_profile_name="cp1",
                compute_group_name="cg1",
                delete_compute_group=False,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )
            drop_cp2_cg1 = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp2_cg1",
                compute_profile_name="cp2",
                compute_group_name="cg1",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )
            drop_cp1_cg1 >> drop_cp2_cg1

        with TaskGroup(group_id='delete_cg2') as tg42:
            drop_cp1_cg2 = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_cg2",
                compute_profile_name="cp1",
                compute_group_name="cg2",
                delete_compute_group=False,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )
            drop_cp2_cg2 = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp2_cg2",
                compute_profile_name="cp2",
                compute_group_name="cg2",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )

            drop_cp1_cg2 >> drop_cp2_cg2

        delete_cg3 = TeradataComputeClusterDecommissionOperator(
            task_id="delete_cg3",
            compute_profile_name="cp2",
            compute_group_name="cg3",
            delete_compute_group=True,
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}",
            trigger_rule=TriggerRule.ALL_DONE
        )
        delete_cg4 = TeradataComputeClusterDecommissionOperator(
            task_id="delete_cg4",
            compute_profile_name="cp1",
            compute_group_name="cg4",
            delete_compute_group=True,
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}",
            trigger_rule=TriggerRule.ALL_DONE
        )
        delete_cg5 = TeradataComputeClusterDecommissionOperator(
            task_id="delete_cg5",
            compute_profile_name="cp1",
            compute_group_name="cg5",
            delete_compute_group=True,
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}",
            trigger_rule=TriggerRule.ALL_DONE
        )
        delete_cg6 = TeradataComputeClusterDecommissionOperator(
            task_id="delete_cg6",
            compute_profile_name="cp2",
            compute_group_name="cg6",
            delete_compute_group=True,
            conn_id="{{ params.conn_id }}",
            timeout="{{ params.timeout }}",
            trigger_rule=TriggerRule.ALL_DONE
        )

        with TaskGroup(group_id='delete_dg') as tg46:
            drop_cp1_dg = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp1_dg",
                compute_profile_name="cp1",
                compute_group_name="dg",
                delete_compute_group=False,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )
            drop_cp2_dg = TeradataComputeClusterDecommissionOperator(
                task_id="drop_cp2_dg",
                compute_profile_name="cp2",
                compute_group_name="dg",
                delete_compute_group=True,
                conn_id="{{ params.conn_id }}",
                timeout="{{ params.timeout }}",
                trigger_rule=TriggerRule.ALL_DONE
            )

            drop_cp1_dg >> drop_cp2_dg

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    (
        start
        >> compute_cluster_provision_operation
        >> [tg0, tg1, tg2, tg3]
        >> tg4
        >> end

    )

    # [END teradata_vantage_lake_compute_cluster_profile_clone_howto_guide]
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
