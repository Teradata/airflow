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
Example Airflow DAG to show usage of TdLoadOperator.

This DAG assumes Airflow Connection with connection id `teradata_default` already exists in locally. It
shows how to load data from a file to a Teradata table, export data from a Teradata table to a file and
transfer data between two Teradata tables (potentially across different databases)
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.tpt import DdlOperator, TdLoadOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START tdload_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_tpt"
CONN_ID = "teradata_default"
SSH_CONN_ID = "ssh_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START ddl_operator_howto_guide_drop_table]
    drop_table = DdlOperator(
        task_id="drop_table",
        sql_stmt="""
                ('DROP TABLE your_table;'),
                ('DROP TABLE your_table_UV;'),
                ('DROP TABLE your_table_ET;'),
                ('DROP TABLE your_table_Log;'),
                ('DROP TABLE your_target_table;'),
                ('DROP TABLE your_target_table_UV;'),
                ('DROP TABLE your_target_table_ET;'),
                ('DROP TABLE your_target_table_Log;')
             """,
        error_list="3706,3803,3807",
    )
    # [END ddl_operator_howto_guide_drop_table]

    # [START ddl_operator_howto_guide_create_table]
    create_table = DdlOperator(
        task_id="create_table",
        sql_stmt="""
                ('CREATE TABLE your_table (
                   FirstName VARCHAR(30),
                   LastName VARCHAR(30),
                   EmployeeNumber VARCHAR(10),
                   Department VARCHAR(20)
                );'),
                ('CREATE TABLE your_target_table (
                   FirstName VARCHAR(30),
                   LastName VARCHAR(30),
                   EmployeeNumber VARCHAR(10),
                   Department VARCHAR(20)
                );')
             """,
        error_list="3706,3803,3807",
    )
    # [END ddl_operator_howto_guide_create_table]

    # [START tdload_operator_howto_guide_load_data_from_txt_file_to_table]
    load_data_from_txt_file_to_table = TdLoadOperator(
        task_id="load_data_from_txt_file_to_table",
        source_file_name="tdload_src_file.txt",
        source_text_delimiter="|",
        source_format="delimited",
        target_table="your_table",
    )
    # [END tdload_operator_howto_guide_load_data_from_txt_file_to_table]

    # [START tdload_operator_howto_guide_export_data_to_a_file]
    export_to_csv_file = TdLoadOperator(
        task_id="export_to_csv_file",
        source_table="your_table",
        target_file_name="your_table.csv",
    )
    # [END tdload_operator_howto_guide_export_data_to_a_file]

    # [START tdload_operator_howto_guide_load_data_from_table_to_table]
    load_data_from_table_to_table = TdLoadOperator(
        task_id="load_from_table_to_table",
        target_table="your_target_table",
        source_table="your_table",
        target_teradata_conn_id=CONN_ID,
    )
    # [END tdload_operator_howto_guide_load_data_from_table_to_table]

    # [START ddl_operator_howto_guide_drop_table_remote]
    drop_table_remote = DdlOperator(
        task_id="drop_table_remote",
        sql_stmt="""
                ('DROP TABLE your_table;'),
                ('DROP TABLE your_table_UV;'),
                ('DROP TABLE your_table_ET;'),
                ('DROP TABLE your_table_Log;'),
                ('DROP TABLE your_target_table;'),
                ('DROP TABLE your_target_table_UV;'),
                ('DROP TABLE your_target_table_ET;'),
                ('DROP TABLE your_target_table_Log;')
             """,
        error_list="3706,3803,3807",
        ssh_conn_id=SSH_CONN_ID,
    )
    # [END ddl_operator_howto_guide_drop_table_remote]

    # [START ddl_operator_howto_guide_create_table_remote]
    create_table_remote = DdlOperator(
        task_id="create_table_remote",
        sql_stmt="""
                ('CREATE TABLE your_table (
                   FirstName VARCHAR(30),
                   LastName VARCHAR(30),
                   EmployeeNumber VARCHAR(10),
                   Department VARCHAR(20)
                );'),
                ('CREATE TABLE your_target_table (
                   FirstName VARCHAR(30),
                   LastName VARCHAR(30),
                   EmployeeNumber VARCHAR(10),
                   Department VARCHAR(20)
                );')
             """,
        error_list="3706,3803,3807",
        ssh_conn_id=SSH_CONN_ID,
    )
    # [END ddl_operator_howto_guide_create_table_remote]

    # [START tdload_operator_howto_guide_load_data_from_txt_file_to_table_remote]
    load_data_from_txt_file_to_table_remote = TdLoadOperator(
        task_id="load_data_from_txt_file_to_table_remote",
        source_file_name="/tmp/tdload_src_file.txt",
        source_text_delimiter="|",
        source_format="delimited",
        target_table="your_table",
        ssh_conn_id=SSH_CONN_ID,
    )
    # [END tdload_operator_howto_guide_load_data_from_txt_file_to_table_remote]

    # [START tdload_operator_howto_guide_export_data_to_a_file_remote]
    export_to_csv_file_remote = TdLoadOperator(
        task_id="export_to_csv_file_remote",
        source_table="your_table",
        target_file_name="your_table.csv",
        ssh_conn_id=SSH_CONN_ID,
    )
    # [END tdload_operator_howto_guide_export_data_to_a_file_remote]

    # [START tdload_operator_howto_guide_load_data_from_table_to_table_remote]
    load_data_from_table_to_table_remote = TdLoadOperator(
        task_id="load_from_table_to_table_remote",
        target_table="your_target_table",
        source_table="your_table",
        target_teradata_conn_id=CONN_ID,
        ssh_conn_id=SSH_CONN_ID,
    )
    # [END tdload_operator_howto_guide_load_data_from_table_to_table_remote]

    (
        drop_table
        >> create_table
        >> load_data_from_txt_file_to_table
        >> export_to_csv_file
        >> load_data_from_table_to_table
        >> drop_table_remote
        >> create_table_remote
        >> load_data_from_txt_file_to_table_remote
        >> export_to_csv_file_remote
        >> load_data_from_table_to_table_remote
    )

    # [END tdload_operator_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
