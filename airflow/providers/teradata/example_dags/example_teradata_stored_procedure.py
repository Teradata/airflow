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
Example Airflow DAG to show basic CRUD operation on teradata database using TeradataOperator

This DAG assumes Airflow Connection with connection id `teradata_default` already exists in locally.
It shows how to run queries as tasks in airflow dags using TeradataOperator..
"""
from __future__ import annotations

from datetime import datetime

import pytest

from airflow import DAG
from airflow.models.baseoperator import chain

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    pytest.skip("Teradata provider pache-airflow-provider-teradata not available", allow_module_level=True)


CONN_ID = "teradata_default"


with DAG(
    dag_id="example_teradata_stored_procedure",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    # [START howto_teradata_operator]

    create = TeradataOperator(
        task_id="table_create",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE my_users,
            FALLBACK (
                user_id decimal(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY (
                    START WITH 1
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 2147483647
                    NO CYCLE),
                user_name VARCHAR(30)
            ) PRIMARY INDEX (user_id);
        """,
    )

    insert = TeradataOperator(
        task_id="insert_rows",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO my_users(user_name) VALUES ('User1');
            INSERT INTO my_users(user_name) VALUES ('User2');
            INSERT INTO my_users(user_name) VALUES ('User3');
            INSERT INTO my_users(user_name) VALUES ('User4');
            INSERT INTO my_users(user_name) VALUES ('User5');
            INSERT INTO my_users(user_name) VALUES ('User6');
            INSERT INTO my_users(user_name) VALUES ('User7');
            INSERT INTO my_users(user_name) VALUES ('User8');
            INSERT INTO my_users(user_name) VALUES ('User9');
            INSERT INTO my_users(user_name) VALUES ('User10');
        """,
    )

    create_proc_insert = TeradataOperator(
        task_id="create_procedure_insert",
        conn_id=CONN_ID,
        sql="""
            CREATE PROCEDURE user_proc_insert (IN v_username VARCHAR(30))
            BEGIN
               INSERT INTO my_users (user_name)
                  VALUES (v_username);
            END;
        """,
    )

    create_proc_insert_execute = TeradataOperator(
        task_id="create_procedure_insert_execute",
        conn_id=CONN_ID,
        sql="""
               CALL user_proc_insert('teradata');
            """,
    )

    create_proc_select = TeradataOperator(
        task_id="create_procedure_select",
        conn_id=CONN_ID,
        sql="""
                CREATE PROCEDURE user_proc_select ()
                DYNAMIC RESULT SETS 1
                BEGIN
                   DECLARE cur1 CURSOR WITH RETURN ONLY FOR
                   SELECT * FROM my_users;
                   OPEN cur1;
                END;
            """,
    )

    create_proc_sel_exe = TeradataOperator(
        task_id="create_proc_sel_exe",
        conn_id=CONN_ID,
        sql="""
                   CALL user_proc_select();
                """,
    )

    read_data = TeradataOperator(
        task_id="read_data",
        conn_id=CONN_ID,
        sql="""
            SELECT TOP 10 * from my_users order by user_id desc;
        """,
    )

    drop_proc_insert = TeradataOperator(
        task_id="drop_proc_insert",
        conn_id=CONN_ID,
        sql="""
            DROP PROCEDURE user_proc_insert;
        """,
    )
    drop_proc_select = TeradataOperator(
        task_id="drop_proc_select",
        conn_id=CONN_ID,
        sql="""
                DROP PROCEDURE user_proc_select;
            """,
    )

    drop = TeradataOperator(
        task_id="drop_table",
        conn_id=CONN_ID,
        sql="""
            DROP TABLE my_users;
        """,
    )

    chain(create, insert, create_proc_insert, create_proc_insert_execute, create_proc_select, create_proc_sel_exe, read_data, drop_proc_insert, drop_proc_select, drop)

    # Make sure create was done before deleting table
    create >> drop

    # [END howto_teradata_operator]
