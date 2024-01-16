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
    dag_id="example_teradata_call_sp",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    # [START howto_teradata_operator_for_sp_call]

    sp_call = TeradataOperator(
        task_id="sp_call",
        conn_id=CONN_ID,
        sql="""
            CALL EAS48_HVR_QLIK_BX_STG_E01.SP_BASE_COMPANY
            (
                'JOB_COMPANY',
                'WF_COMPANY01',
                'WF_COMPANY',
                CAST('02022023' AS DATE FORMAT 'MMDDYYYY'),
                'E01-800',
                CAST('01011900' AS DATE FORMAT 'MMDDYYYY'),
                'init'
            );
        """,
    )

    chain(sp_call)

    # [END howto_teradata_operator_for_sp_call]
