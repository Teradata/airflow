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

"""Switch XCom table to use ``run_id``.

Revision ID: c306b5b5ae4a
Revises: a3bcd0914482
Create Date: 2022-01-19 03:20:35.329037
"""
from typing import Sequence

from alembic import op
from sqlalchemy import Column, Integer, LargeBinary, MetaData, Table, and_, select

from airflow.migrations.db_types import TIMESTAMP, StringID

# Revision identifiers, used by Alembic.
revision = "c306b5b5ae4a"
down_revision = "a3bcd0914482"
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


metadata = MetaData()


def _get_new_xcom_columns() -> Sequence[Column]:
    return [
        Column("dagrun_id", Integer(), nullable=False),
        Column("task_id", StringID(), nullable=False),
        Column("key", StringID(length=512), nullable=False),
        Column("value", LargeBinary),
        Column("timestamp", TIMESTAMP, nullable=False),
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
    ]


def _get_old_xcom_columns() -> Sequence[Column]:
    return [
        Column("key", StringID(length=512), nullable=False),
        Column("value", LargeBinary),
        Column("timestamp", TIMESTAMP, nullable=False),
        Column("task_id", StringID(), nullable=False),
        Column("dag_id", StringID(), nullable=False),
        Column("execution_date", StringID(), nullable=False),
    ]


def _get_dagrun_table() -> Table:
    return Table(
        "dag_run",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
        Column("execution_date", TIMESTAMP, nullable=False),
    )


def upgrade():
    """Switch XCom table to use run_id.

    For performance reasons, this is done by creating a new table with needed
    data pre-populated, adding back constraints we need, and renaming it to
    replace the existing XCom table.
    """
    conn = op.get_bind()
    is_sqlite = conn.dialect.name == "sqlite"

    op.create_table("__airflow_tmp_xcom", *_get_new_xcom_columns())

    xcom = Table("xcom", metadata, *_get_old_xcom_columns())
    dagrun = _get_dagrun_table()
    query = select(
        [
            dagrun.c.id,
            xcom.c.task_id,
            xcom.c.key,
            xcom.c.value,
            xcom.c.timestamp,
            xcom.c.dag_id,
            dagrun.c.run_id,
        ],
    ).select_from(
        xcom.join(
            right=dagrun,
            onclause=and_(
                xcom.c.dag_id == dagrun.c.dag_id,
                xcom.c.execution_date == dagrun.c.execution_date,
            ),
        ),
    )
    op.execute(f"INSERT INTO __airflow_tmp_xcom {query.selectable.compile(op.get_bind())}")

    if is_sqlite:
        op.execute("PRAGMA foreign_keys=off")
    op.drop_table("xcom")
    if is_sqlite:
        op.execute("PRAGMA foreign_keys=on")
    op.rename_table("__airflow_tmp_xcom", "xcom")

    with op.batch_alter_table("xcom") as batch_op:
        batch_op.create_primary_key("xcom_pkey", ["dagrun_id", "task_id", "key"])
        batch_op.create_index("idx_xcom_key", ["key"])
        batch_op.create_index("idx_xcom_ti_id", ["dag_id", "task_id", "run_id"])


def downgrade():
    """Switch XCom table back to use execution_date.

    Basically an inverse operation.
    """
    op.create_table("__airflow_tmp_xcom", *_get_old_xcom_columns())

    xcom = Table("xcom", metadata, *_get_new_xcom_columns())
    dagrun = _get_dagrun_table()
    query = select(
        [
            xcom.c.key,
            xcom.c.value,
            xcom.c.timestamp,
            xcom.c.task_id,
            xcom.c.dag_id,
            dagrun.c.execution_date,
        ],
    ).select_from(
        xcom.join(
            right=dagrun,
            onclause=and_(
                xcom.c.dag_id == dagrun.c.dag_id,
                xcom.c.run_id == dagrun.c.run_id,
            ),
        ),
    )
    op.execute(f"INSERT INTO __airflow_tmp_xcom {query.selectable.compile(op.get_bind())}")

    op.drop_table("xcom")
    op.rename_table("__airflow_tmp_xcom", "xcom")
    op.create_primary_key("xcom_pkey", "xcom", ["dag_id", "task_id", "execution_date", "key"])
