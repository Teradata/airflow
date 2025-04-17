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
This is an example DAG for the use of the SQLExecuteQueryOperator with Hive.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "example_hive"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    default_args={"conn_id": "my_hive_conn"},
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_hive]

    create_table_hive_task = SQLExecuteQueryOperator(
        task_id="create_table_hive",
        sql="create table hive_example(a string, b int) partitioned by(c int)",
    )

    # [END howto_operator_hive]

    alter_table_hive_task = SQLExecuteQueryOperator(
        task_id="alter_table_hive",
        sql="alter table hive_example add partition(c=1)",
    )

    insert_data_hive_task = SQLExecuteQueryOperator(
        task_id="insert_data_hive",
        sql="insert into hive_example partition(c=1) values('a', 1), ('a', 2),('b',3)",
    )

    select_data_hive_task = SQLExecuteQueryOperator(
        task_id="select_data_hive",
        sql="select * from hive_example",
    )

    drop_table_hive_task = SQLExecuteQueryOperator(
        task_id="drop_table_hive",
        sql="drop table hive_example",
    )

    (
        create_table_hive_task
        >> alter_table_hive_task
        >> insert_data_hive_task
        >> select_data_hive_task
        >> drop_table_hive_task
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
