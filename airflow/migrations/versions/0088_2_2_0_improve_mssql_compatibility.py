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
"""Improve MSSQL compatibility.

Revision ID: 83f031fd9f1c
Revises: ccde3e26fe78
Create Date: 2021-04-06 12:22:02.197726

"""

from __future__ import annotations

from collections import defaultdict

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import mssql

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = "83f031fd9f1c"
down_revision = "ccde3e26fe78"
branch_labels = None
depends_on = None
airflow_version = "2.2.0"


def is_table_empty(conn, table_name):
    """
    Check if the MS SQL table is empty.

    :param conn: SQL connection object
    :param table_name: table name
    :return: Booelan indicating if the table is present
    """
    return conn.execute(text(f"select TOP 1 * from {table_name}")).first() is None


def get_table_constraints(conn, table_name) -> dict[tuple[str, str], list[str]]:
    """
    Get tables primary and unique constraints.

    This function return primary and unique constraint
    along with column name. some tables like task_instance
    is missing primary key constraint name and the name is
    auto-generated by sql server. so this function helps to
    retrieve any primary or unique constraint name.

    :param conn: sql connection object
    :param table_name: table name
    :return: a dictionary of ((constraint name, constraint type), column name) of table
    """
    query = text(
        f"""SELECT tc.CONSTRAINT_NAME , tc.CONSTRAINT_TYPE, ccu.COLUMN_NAME
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     WHERE tc.TABLE_NAME = '{table_name}' AND
     (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' or UPPER(tc.CONSTRAINT_TYPE) = 'UNIQUE')
    """
    )
    result = conn.execute(query).fetchall()
    constraint_dict = defaultdict(list)
    for constraint, constraint_type, column in result:
        constraint_dict[(constraint, constraint_type)].append(column)
    return constraint_dict


def drop_column_constraints(operator, column_name, constraint_dict):
    """
    Drop a primary key or unique constraint.

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if column_name in columns:
            if constraint[1].lower().startswith("primary"):
                operator.drop_constraint(constraint[0], type_="primary")
            elif constraint[1].lower().startswith("unique"):
                operator.drop_constraint(constraint[0], type_="unique")


def create_constraints(operator, column_name, constraint_dict):
    """
    Create a primary key or unique constraint.

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if column_name in columns:
            if constraint[1].lower().startswith("primary"):
                operator.create_primary_key(constraint_name=constraint[0], columns=columns)
            elif constraint[1].lower().startswith("unique"):
                operator.create_unique_constraint(constraint_name=constraint[0], columns=columns)


def _is_timestamp(conn, table_name, column_name):
    query = text(
        f"""SELECT
    TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE
    FROM SYS.COLUMNS C
    JOIN SYS.TYPES T
    ON C.USER_TYPE_ID=T.USER_TYPE_ID
    WHERE C.OBJECT_ID=OBJECT_ID('{table_name}') and C.NAME='{column_name}';
    """
    )
    column_type = conn.execute(query).fetchone()[0]
    return column_type == "timestamp"


def recreate_mssql_ts_column(conn, op, table_name, column_name):
    """Drop the timestamp column and recreate it as datetime or datetime2(6)."""
    if _is_timestamp(conn, table_name, column_name) and is_table_empty(conn, table_name):
        with op.batch_alter_table(table_name) as batch_op:
            constraint_dict = get_table_constraints(conn, table_name)
            drop_column_constraints(batch_op, column_name, constraint_dict)
            batch_op.drop_column(column_name=column_name)
            batch_op.add_column(sa.Column(column_name, TIMESTAMP, nullable=False))
            create_constraints(batch_op, column_name, constraint_dict)


def alter_mssql_datetime_column(conn, op, table_name, column_name, nullable):
    """Update the datetime column to datetime2(6)."""
    op.alter_column(
        table_name=table_name,
        column_name=column_name,
        type_=mssql.DATETIME2(precision=6),
        nullable=nullable,
    )


def upgrade():
    """Improve compatibility with MSSQL backend."""
    conn = op.get_bind()
    if conn.dialect.name != "mssql":
        return
    recreate_mssql_ts_column(conn, op, "dag_code", "last_updated")
    recreate_mssql_ts_column(conn, op, "rendered_task_instance_fields", "execution_date")
    alter_mssql_datetime_column(conn, op, "serialized_dag", "last_updated", False)
    op.alter_column(table_name="xcom", column_name="timestamp", type_=TIMESTAMP, nullable=False)
    with op.batch_alter_table("task_reschedule") as task_reschedule_batch_op:
        task_reschedule_batch_op.alter_column(column_name="end_date", type_=TIMESTAMP, nullable=False)
        task_reschedule_batch_op.alter_column(column_name="reschedule_date", type_=TIMESTAMP, nullable=False)
        task_reschedule_batch_op.alter_column(column_name="start_date", type_=TIMESTAMP, nullable=False)
    with op.batch_alter_table("task_fail") as task_fail_batch_op:
        task_fail_batch_op.drop_index("idx_task_fail_dag_task_date")
        task_fail_batch_op.alter_column(column_name="execution_date", type_=TIMESTAMP, nullable=False)
        task_fail_batch_op.create_index(
            "idx_task_fail_dag_task_date", ["dag_id", "task_id", "execution_date"], unique=False
        )
    with op.batch_alter_table("task_instance") as task_instance_batch_op:
        task_instance_batch_op.drop_index("ti_state_lkp")
        task_instance_batch_op.create_index(
            "ti_state_lkp", ["dag_id", "task_id", "execution_date", "state"], unique=False
        )
    constraint_dict = get_table_constraints(conn, "dag_run")
    for constraint, columns in constraint_dict.items():
        if "dag_id" in columns:
            if constraint[1].lower().startswith("unique"):
                op.drop_constraint(constraint[0], "dag_run", type_="unique")
    # create filtered indexes
    conn.execute(
        text(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_execution_date
                ON dag_run(dag_id,execution_date)
                WHERE dag_id IS NOT NULL and execution_date is not null"""
        )
    )
    conn.execute(
        text(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_run_id
                 ON dag_run(dag_id,run_id)
                 WHERE dag_id IS NOT NULL and run_id is not null"""
        )
    )


def downgrade():
    """Reverse MSSQL backend compatibility improvements."""
    conn = op.get_bind()
    if conn.dialect.name != "mssql":
        return
    op.alter_column(table_name="xcom", column_name="timestamp", type_=TIMESTAMP, nullable=True)
    with op.batch_alter_table("task_reschedule") as task_reschedule_batch_op:
        task_reschedule_batch_op.alter_column(column_name="end_date", type_=TIMESTAMP, nullable=True)
        task_reschedule_batch_op.alter_column(column_name="reschedule_date", type_=TIMESTAMP, nullable=True)
        task_reschedule_batch_op.alter_column(column_name="start_date", type_=TIMESTAMP, nullable=True)
    with op.batch_alter_table("task_fail") as task_fail_batch_op:
        task_fail_batch_op.drop_index("idx_task_fail_dag_task_date")
        task_fail_batch_op.alter_column(column_name="execution_date", type_=TIMESTAMP, nullable=False)
        task_fail_batch_op.create_index(
            "idx_task_fail_dag_task_date", ["dag_id", "task_id", "execution_date"], unique=False
        )
    with op.batch_alter_table("task_instance") as task_instance_batch_op:
        task_instance_batch_op.drop_index("ti_state_lkp")
        task_instance_batch_op.create_index(
            "ti_state_lkp", ["dag_id", "task_id", "execution_date"], unique=False
        )
    op.create_unique_constraint("UQ__dag_run__dag_id_run_id", "dag_run", ["dag_id", "run_id"])
    op.create_unique_constraint("UQ__dag_run__dag_id_execution_date", "dag_run", ["dag_id", "execution_date"])
    op.drop_index("idx_not_null_dag_id_execution_date", table_name="dag_run")
    op.drop_index("idx_not_null_dag_id_run_id", table_name="dag_run")
