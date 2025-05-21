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
Example Airflow DAG to show usage of BteqOperator.

This DAG assumes Airflow Connection with connection id `TTU_DEFAULT` already exists in locally. It
shows how to use Teradata BTEQ commands with BteqOperator as tasks in
airflow dags using BteqeOperator.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.bteq import BteqOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START bteq_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_bteq"
CONN_ID = "teradata_default"
SSH_CONN_ID = "ssh_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID, "ssh_conn_id": SSH_CONN_ID},
) as dag:
    # [START bteq_operator_howto_guide_create_table]
    create_table = BteqOperator(
        task_id="create_table",
        bteq=r"""
        CREATE SET TABLE my_employees (
          emp_id INT,
          emp_name VARCHAR(100),
          dept VARCHAR(50)
        ) PRIMARY INDEX (emp_id);
        """,
    )
    # [END bteq_operator_howto_guide_create_table]

    # [START bteq_operator_howto_guide_populate_table]
    populate_table = BteqOperator(
        task_id="populate_table",
        bteq=r"""
        INSERT INTO my_employees VALUES (1, 'John Doe', 'IT');
        INSERT INTO my_employees VALUES (2, 'Jane Smith', 'HR');
        """,
    )
    # [END bteq_operator_howto_guide_populate_table]

    # [START bteq_operator_howto_guide_export_data_to_a_file]
    export_to_a_file = BteqOperator(
        task_id="export_to_a_file",
        bteq=r"""
        .EXPORT FILE = employees_output.txt;
        SELECT * FROM my_employees;
        .EXPORT RESET;
        """,
    )
    # [END bteq_operator_howto_guide_export_data_to_a_file]

    # [START bteq_operator_howto_guide_get_it_employees]
    get_it_employees = BteqOperator(
        task_id="get_it_employees",
        bteq=r"""
        SELECT * FROM my_employees WHERE dept = 'IT';
        """,
    )
    # [END bteq_operator_howto_guide_get_it_employees]

    # [START bteq_operator_howto_guide_conditional_logic]
    cond_logic = BteqOperator(
        task_id="cond_logic",
        bteq=r"""
        .IF ERRORCODE <> 0 THEN .GOTO handle_error;

        SELECT COUNT(*) FROM my_employees;

        .LABEL handle_error;
        """,
    )
    # [END bteq_operator_howto_guide_conditional_logic]

    # [START bteq_operator_howto_guide_error_handling]
    error_handling = BteqOperator(
        task_id="error_handling",
        bteq=r"""
        DROP TABLE my_temp;
        .IF ERRORCODE = 3807 THEN .GOTO table_not_found;
        SELECT 'Table dropped successfully.';
        .GOTO end;

        .LABEL table_not_found;
        SELECT 'Table not found - continuing execution';
        .LABEL end;
        .LOGOFF;
        .QUIT 0;
        """,
    )
    # [END bteq_operator_howto_guide_error_handling]

    # [START bteq_operator_howto_guide_drop_table]
    drop_table = BteqOperator(
        task_id="drop_table",
        bteq=r"""
        DROP TABLE my_employees;
        .IF ERRORCODE = 3807 THEN .GOTO end;

        .LABEL end;
        .LOGOFF;
        .QUIT 0;
        """,
    )
    # [END bteq_operator_howto_guide_drop_table]

    (
        create_table
        >> populate_table
        >> export_to_a_file
        >> get_it_employees
        >> cond_logic
        >> error_handling
        >> drop_table
    )

    # [END bteq_operator_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
