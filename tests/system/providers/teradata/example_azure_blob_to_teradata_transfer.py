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
Example Airflow DAG to show usage of AzureBlobStorageToTeradataOperator

The transfer operator transfers CSV, JSON and PARQUET data format files from Azure Blob storage to
teradata tables. The example DAG below assumes Airflow Connections with connection ids `teradata_default`
and `wasb_default` exists already. It creates tables with data from Azure blob location, returns
numbers of rows inserted into table and then drops this table.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.providers.teradata.transfers.s3_to_teradata import S3ToTeradataOperator

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.azure_blob_to_teradata import AzureBlobStorageToTeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

# [START azure_blob_to_teradata_transfer_operator_howto_guide]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_azure_blob_to_teradata_transfer_operator"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_default"},
) as dag:
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_csv_data_azure_blob_to_teradata]
    transfer_data_csv = AzureBlobStorageToTeradataOperator(
        task_id="transfer_data_blob_to_teradata_csv",
        blob_source_key="/az/akiaxox5jikeotfww4ul.blob.core.windows.net/td-usgs/CSVDATA/",
        teradata_table="example_blob_teradata_csv",
        wasb_conn_id="wasb_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    read_data_table_csv = TeradataOperator(
        task_id="read_data_table_csv",
        conn_id=CONN_ID,
        sql="""
                    SELECT * from example_blob_teradata_csv;
                """,
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_csv_data_azure_blob_to_teradata]

    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_parquet_data_azure_blob_to_teradata]
    (
        transfer_data_csv,
        read_data_table_csv,
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
