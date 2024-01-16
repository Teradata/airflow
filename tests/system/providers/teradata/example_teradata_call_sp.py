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
Example use of Teradata related operators.
"""
from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_operator_howto_guide]


# call_sp is the example of stored prcedure call task created by instantiating the Teradata Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_default"},
) as dag:

    # [START teradata_operator_howto_guide_sp_call]
    call_sp = TeradataOperator(
        task_id="call_sp",
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
        trigger_rule="all_done",
    )
    # [END teradata_operator_howto_guide_sp_call]


    (
        call_sp
    )

    # [END teradata_operator_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
